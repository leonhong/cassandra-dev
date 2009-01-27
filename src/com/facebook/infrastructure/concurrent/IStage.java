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

package com.facebook.infrastructure.concurrent;

import java.util.concurrent.*;

/**
 * An abstraction for stages as described in the SEDA paper by Matt Welsh. 
 * For reference to the paper look over here 
 * <a href="http://www.eecs.harvard.edu/~mdw/papers/seda-sosp01.pdf">SEDA: An Architecture for WellConditioned,
   Scalable Internet Services</a>.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface IStage 
{
    public String getName();
    
    /**
     * This method is used to execute a peice of code on
     * this stage. The idea is that the <i>run()</i> method
     * of this Runnable instance is invoked on a thread from a
     * thread pool that belongs to this stage.
     * @param runnable instance whose run() method needs to be invoked.
     */
    public void execute(Runnable runnable);
    
    /**
     * This method is used to execute a peice of code on
     * this stage which returns a Future pointer. The idea
     * is that the <i>call()</i> method of this Runnable 
     * instance is invoked on a thread from a thread pool 
     * that belongs to this stage.
     
     * @param callable instance that needs to be invoked.
     * @return
     */
    public Future<Object> execute(Callable<Object> callable);
    
    /**
     * This method is used to submit tasks to this stage
     * that execute periodically. 
     * 
     * @param command the task to execute.
     * @param initialDelay the time to delay first execution 
     * @param unit the time unit of the initialDelay and period parameters 
     * @return
     */
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit); 
      
    /**
     * This method is used to submit tasks to this stage
     * that execute periodically. 
     * @param command the task to execute.
     * @param initialDelay the time to delay first execution
     * @param period the period between successive executions
     * @param unit the time unit of the initialDelay and period parameters 
     * @return
     */
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit); 
    
    /**
     * This method is used to submit tasks to this stage
     * that execute periodically. 
     * @param command the task to execute.
     * @param initialDelay the time to delay first execution
     * @param delay  the delay between the termination of one execution and the commencement of the next.
     * @param unit the time unit of the initialDelay and delay parameters 
     * @return
     */
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);
    
    /**
     * Shutdown the stage. All the threads of this stage
     * are forcefully shutdown. Any pending tasks on this
     * stage could be dropped or the stage could wait for 
     * these tasks to be completed. This is however an 
     * implementation detail.
     */
    public void shutdown();  
    
    /**
     * Checks if the stage has been shutdown.
     * @return
     */
    public boolean isShutdown();
    
    /**
     * This method returns the number of tasks that are 
     * pending on this stage to be executed.
     * @return
     */
    public long getTaskCount();
}
