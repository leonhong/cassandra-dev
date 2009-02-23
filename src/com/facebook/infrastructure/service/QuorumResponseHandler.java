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

package com.facebook.infrastructure.service;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class QuorumResponseHandler<T> implements IAsyncCallback
{
    private static Logger logger_ = Logger.getLogger( QuorumResponseHandler.class );
    private Lock lock_ = new ReentrantLock();
    private Condition condition_;
    private int responseCount_;
    private List<Message<byte[]>> responses_ = new ArrayList<Message<byte[]>>();
    private IResponseResolver<T> responseResolver_;
    private AtomicBoolean done_ = new AtomicBoolean(false);
    
    public QuorumResponseHandler(int responseCount, IResponseResolver<T> responseResolver)
    {        
        condition_ = lock_.newCondition();
        responseCount_ = responseCount;
        responseResolver_ =  responseResolver;
    }
    public void  setResponseCount(int responseCount)
    {
        responseCount_ = responseCount;
    }
    public T get() throws TimeoutException, DigestMismatchException, InterruptedException {
        long startTime = System.currentTimeMillis();
    	lock_.lock();
        try
        {            
            boolean bVal = true;            
            if ( !done_.get() )
            {
                bVal = condition_.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            }

            if ( !bVal && !done_.get() )
            {
                throw new TimeoutException("Operation timed out - received only " +  responses_.size() + " responses from [" + StringUtils.join(responders()) + "]");
            }
        }
        finally
        {
            lock_.unlock();
            for(Message response : responses_)
            {
            	MessagingService.removeRegisteredCallback( response.getMessageId() );
            }
        }
        logger_.debug("QuorumResponseHandler: " + (System.currentTimeMillis() - startTime) + " ms; " + " responses from [" + StringUtils.join(responders()) + "]");

    	return responseResolver_.resolve( responses_);
    }

    private EndPoint[] responders() {
        EndPoint[] a = new EndPoint[responses_.size()];
        for (int i = 0; i < responses_.size(); i++) {
            a[i] = responses_.get(i).getFrom();
        }
        return a;
    }
    
    public void response(Message<byte[]> message)
    {
        lock_.lock();
        try
        {
        	int majority = (responseCount_ >> 1) + 1;            
            if ( !done_.get() )
            {
            	responses_.add( message );
            	if ( responses_.size() >= majority && responseResolver_.isDataPresent(responses_))
            	{
            		done_.set(true);
            		condition_.signal();            	
            	}
            }
        }
        finally
        {
            lock_.unlock();
        }
    }
}
