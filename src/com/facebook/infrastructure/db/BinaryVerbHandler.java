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

import com.facebook.infrastructure.db.RowMutationVerbHandler.RowMutationContext;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.utils.LogUtil;
import org.apache.log4j.Logger;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class BinaryVerbHandler implements IVerbHandler<byte[]>
{
    private static Logger logger_ = Logger.getLogger(BinaryVerbHandler.class);    
    /* We use this so that we can reuse the same row mutation context for the mutation. */
    private static ThreadLocal<RowMutationContext> tls_ = new InheritableThreadLocal<RowMutationContext>();
    
    public void doVerb(Message<byte[]> message)
    { 
        byte[] bytes = message.getMessageBody();
        /* Obtain a Row Mutation Context from TLS */
        RowMutationContext rowMutationCtx = tls_.get();
        if ( rowMutationCtx == null )
        {
            rowMutationCtx = new RowMutationContext();
            tls_.set(rowMutationCtx);
        }                
        rowMutationCtx.buffer_.reset(bytes, bytes.length);
        
	    try
	    {
            RowMutation rm = RowMutation.serializer().deserialize(rowMutationCtx.buffer_);
            	    
            rowMutationCtx.row_.key(rm.key());
            rm.load(rowMutationCtx.row_);
	
	    }        
	    catch ( Exception e )
	    {
	        logger_.debug(LogUtil.throwableToString(e));            
	    }        
    }

}
