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

package com.facebook.infrastructure.net;

import java.util.List;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.service.QuorumResponseHandler;
import com.facebook.infrastructure.utils.LogUtil;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class AsyncResult implements IAsyncResult
{
    private static Logger logger_ = Logger.getLogger( AsyncResult.class );
    private Object[] result_ = new Object[0];    
    private AtomicBoolean done_ = new AtomicBoolean(false);
    private Lock lock_ = new ReentrantLock();
    private Condition condition_;

    public AsyncResult()
    {        
        condition_ = lock_.newCondition();
    }    
    
    public Object[] get()
    {
        lock_.lock();
        try
        {
            if ( !done_.get() )
            {
                condition_.await();                    
            }
        }
        catch ( InterruptedException ex )
        {
            logger_.warn( LogUtil.throwableToString(ex) );
        }
        finally
        {
            lock_.unlock();            
        }        
        return result_;
    }
    
    public boolean isDone()
    {
        return done_.get();
    }
    
    public Object[] get(long timeout, TimeUnit tu) throws TimeoutException
    {
        lock_.lock();
        try
        {            
            boolean bVal = true;
            try
            {
                if ( !done_.get() )
                {                    
                    bVal = condition_.await(timeout, tu);
                }
            }
            catch ( InterruptedException ex )
            {
                logger_.warn( LogUtil.throwableToString(ex) );
            }
            
            if ( !bVal && !done_.get() )
            {                                           
                throw new TimeoutException("Operation timed out.");
            }
        }
        finally
        {
            lock_.unlock();      
        }
        return result_;
    }
    
    void result(Object[] result)
    {        
        try
        {
            lock_.lock();
            if ( !done_.get() )
            {
                result_ = result;
                done_.set(true);
                condition_.signal();
            }
        }
        finally
        {
            lock_.unlock();
        }        
    }    
}
