/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.infrastructure.db;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.facebook.infrastructure.concurrent.*;
import com.facebook.infrastructure.db.HintedHandOffManager.HintedHandOff;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.service.IComponentShutdown;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.BloomFilter;
import com.facebook.infrastructure.utils.LogUtil;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class MinorCompactionManager implements IComponentShutdown
{
    private static MinorCompactionManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(MinorCompactionManager.class);
    final static long intervalInMins_ = 5;

    public static MinorCompactionManager instance()
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                    instance_ = new MinorCompactionManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }

    class FileCompactor implements Runnable
    {
        private ColumnFamilyStore columnFamilyStore_;

        FileCompactor(ColumnFamilyStore columnFamilyStore)
        {
        	columnFamilyStore_ = columnFamilyStore;
        }

        public void run()
        {
            try
            {
                logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
            	columnFamilyStore_.doCompaction(null);
                logger_.debug("Finished compaction ..."+columnFamilyStore_.columnFamily_);
            }
            catch (IOException e)
            {
                logger_.debug( LogUtil.throwableToString(e) );
            }
            catch (Throwable th)
            {
                logger_.error( LogUtil.throwableToString(th) );
            }
        }
    }

    class FileCompactor2 implements Callable<BloomFilter.CountingBloomFilter>
    {
        private ColumnFamilyStore columnFamilyStore_;
        private List<Range> ranges_;
        private EndPoint target_;
        private List<String> fileList_;

        FileCompactor2(ColumnFamilyStore columnFamilyStore, List<Range> ranges)
        {
            columnFamilyStore_ = columnFamilyStore;
            ranges_ = ranges;
        }
        
        FileCompactor2(ColumnFamilyStore columnFamilyStore, List<Range> ranges, EndPoint target,List<String> fileList)
        {
            columnFamilyStore_ = columnFamilyStore;
            ranges_ = ranges;
            target_ = target;
            fileList_ = fileList;
        }

        public BloomFilter.CountingBloomFilter call()
        {
        	BloomFilter.CountingBloomFilter result = null;
            try
            {
                logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
                result = columnFamilyStore_.doRangeAntiCompaction(ranges_, target_,fileList_);
                logger_.debug("Finished compaction ..."+columnFamilyStore_.columnFamily_);
            }
            catch (IOException e)
            {
                logger_.debug( LogUtil.throwableToString(e) );
            }
            return result;
        }
    }

    class OnDemandCompactor implements Callable<BloomFilter.CountingBloomFilter>
    {
        private ColumnFamilyStore columnFamilyStore_;
        private long skip_ = 0L;

        OnDemandCompactor(ColumnFamilyStore columnFamilyStore, long skip)
        {
            columnFamilyStore_ = columnFamilyStore;
            skip_ = skip;
        }

        public BloomFilter.CountingBloomFilter call()
        {
        	BloomFilter.CountingBloomFilter result = null;
            try
            {
                logger_.debug("Started  Major compaction ..."+columnFamilyStore_.columnFamily_);
                result = columnFamilyStore_.doMajorCompaction(skip_);
                logger_.debug("Finished Major compaction ..."+columnFamilyStore_.columnFamily_);
            }
            catch (IOException e)
            {
                logger_.debug( LogUtil.throwableToString(e) );
            }
            return result;
        }
    }

    class CleanupCompactor implements Runnable
    {
        private ColumnFamilyStore columnFamilyStore_;

        CleanupCompactor(ColumnFamilyStore columnFamilyStore)
        {
        	columnFamilyStore_ = columnFamilyStore;
        }

        public void run()
        {
            try
            {
                logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
            	columnFamilyStore_.doCleanupCompaction();
                logger_.debug("Finished compaction ..."+columnFamilyStore_.columnFamily_);
            }
            catch (IOException e)
            {
                logger_.debug( LogUtil.throwableToString(e) );
            }
            catch (Throwable th)
            {
                logger_.error( LogUtil.throwableToString(th) );
            }
        }
    }
    
    
    private ScheduledExecutorService compactor_ = new DebuggableScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("MINOR-COMPACTION-POOL"));

    public MinorCompactionManager()
    {
    	StorageService.instance().registerComponentForShutdown(this);
	}

    public void shutdown()
    {
    	compactor_.shutdownNow();
    }

    public void submitPeriodicCompaction(ColumnFamilyStore columnFamilyStore)
    {        
    	compactor_.scheduleWithFixedDelay(new FileCompactor(columnFamilyStore), MinorCompactionManager.intervalInMins_,
    			MinorCompactionManager.intervalInMins_, TimeUnit.MINUTES);       
    }

    public void submit(ColumnFamilyStore columnFamilyStore)
    {
        compactor_.submit(new FileCompactor(columnFamilyStore));
    }
    
    public void submitCleanup(ColumnFamilyStore columnFamilyStore)
    {
        compactor_.submit(new CleanupCompactor(columnFamilyStore));
    }

    public Future<BloomFilter.CountingBloomFilter> submit(ColumnFamilyStore columnFamilyStore, List<Range> ranges, EndPoint target, List<String> fileList)
    {
        return compactor_.submit( new FileCompactor2(columnFamilyStore, ranges, target, fileList) );
    } 

    public Future<BloomFilter.CountingBloomFilter> submit(ColumnFamilyStore columnFamilyStore, List<Range> ranges)
    {
        return compactor_.submit( new FileCompactor2(columnFamilyStore, ranges) );
    }

    public Future<BloomFilter.CountingBloomFilter> submitMajor(ColumnFamilyStore columnFamilyStore, List<Range> ranges, long skip)
    {
        return compactor_.submit( new OnDemandCompactor(columnFamilyStore, skip) );
    }
}
