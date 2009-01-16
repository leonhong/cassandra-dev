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

import com.facebook.infrastructure.concurrent.DebuggableScheduledThreadPoolExecutor;
import com.facebook.infrastructure.concurrent.ThreadFactoryImpl;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.service.IComponentShutdown;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.CountingBloomFilter;
import com.facebook.infrastructure.utils.Filter;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class CompactionManager implements IComponentShutdown
{
    private static CompactionManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(CompactionManager.class);
    final static long intervalInMins_ = 5;

    public static CompactionManager instance()
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                    instance_ = new CompactionManager();
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
            columnFamilyStore_.compact(new ColumnFamilyCompactor.Wrapper() {
                public Filter run() throws IOException {
                    return ColumnFamilyCompactor.doCompaction(columnFamilyStore_, null);
                }
            });
        }
    }

    class FileCompactor2 implements Callable<CountingBloomFilter>
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

        public CountingBloomFilter call()
        {
        	return (CountingBloomFilter) columnFamilyStore_.compact(new ColumnFamilyCompactor.Wrapper() {
                public Filter run() throws IOException {
                    return ColumnFamilyCompactor.doRangeOnlyAntiCompaction(columnFamilyStore_, columnFamilyStore_.getAllSSTablesOnDisk(), ranges_, target_, ColumnFamilyStore.BUFFER_SIZE, fileList_, null);
                }
            });
        }
    }

    class OnDemandCompactor implements Callable<CountingBloomFilter>
    {
        private ColumnFamilyStore columnFamilyStore_;
        private long skip_ = 0L;

        OnDemandCompactor(ColumnFamilyStore columnFamilyStore, long skip)
        {
            columnFamilyStore_ = columnFamilyStore;
            skip_ = skip;
        }

        public CountingBloomFilter call()
        {
            List<String> filesInternal = columnFamilyStore_.getAllSSTablesOnDisk();
            final List<String> files;
            if( skip_ > 0L )
            {
                files = new ArrayList<String>();
                for ( String file : filesInternal )
                {
                    File f = new File(file);
                    if( f.length() < skip_ *1024L*1024L*1024L )
                    {
                        files.add(file);
                    }
                }
            }
            else
            {
                files = filesInternal;
            }
            return (CountingBloomFilter) columnFamilyStore_.compact(new ColumnFamilyCompactor.Wrapper() {
                public Filter run() throws IOException {
                    return ColumnFamilyCompactor.doRangeCompaction(columnFamilyStore_, files, null, ColumnFamilyStore.BUFFER_SIZE);
                }
            });
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
            columnFamilyStore_.compact(new ColumnFamilyCompactor.Wrapper() {
                public Filter run() throws IOException {
                    for(String file: columnFamilyStore_.getAllSSTablesOnDisk()) {
                        columnFamilyStore_.doCleanup(file);
                    }
                    return null;
                }
            });
        }
    }
    
    
    private ScheduledExecutorService compactor_ = new DebuggableScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("MINOR-COMPACTION-POOL"));

    public CompactionManager()
    {
    	StorageService.instance().registerComponentForShutdown(this);
	}

    public void shutdown()
    {
    	compactor_.shutdownNow();
    }

    public void submitPeriodicCompaction(ColumnFamilyStore columnFamilyStore)
    {        
    	compactor_.scheduleWithFixedDelay(new FileCompactor(columnFamilyStore), CompactionManager.intervalInMins_,
    			CompactionManager.intervalInMins_, TimeUnit.MINUTES);
    }

    public void submit(ColumnFamilyStore columnFamilyStore)
    {
        compactor_.submit(new FileCompactor(columnFamilyStore));
    }
    
    public void submitCleanup(ColumnFamilyStore columnFamilyStore)
    {
        compactor_.submit(new CleanupCompactor(columnFamilyStore));
    }

    public Future<CountingBloomFilter> submit(ColumnFamilyStore columnFamilyStore, List<Range> ranges, EndPoint target, List<String> fileList)
    {
        return compactor_.submit( new FileCompactor2(columnFamilyStore, ranges, target, fileList) );
    } 

    public Future<CountingBloomFilter> submit(ColumnFamilyStore columnFamilyStore, List<Range> ranges)
    {
        return compactor_.submit( new FileCompactor2(columnFamilyStore, ranges) );
    }

    public Future<CountingBloomFilter> submitMajor(ColumnFamilyStore columnFamilyStore, List<Range> ranges, long skip)
    {
        return compactor_.submit( new OnDemandCompactor(columnFamilyStore, skip) );
    }
}
