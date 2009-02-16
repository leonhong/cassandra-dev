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

import com.facebook.infrastructure.concurrent.StageManager;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.LogUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class RowMutationVerbHandler implements IVerbHandler
{
    protected static class RowMutationContext
    {
        protected Row row_ = new Row();
        protected DataInputBuffer buffer_ = new DataInputBuffer();
    }
    
    private static Logger logger_ = Logger.getLogger(RowMutationVerbHandler.class);     
    /* We use this so that we can reuse the same row mutation context for the mutation. */
    private static ThreadLocal<RowMutationContext> tls_ = new InheritableThreadLocal<RowMutationContext>();
    
    public void doVerb(Message message)
    {
        logger_.info( "ROW MUTATION STAGE: " + StageManager.getStageTaskCount(StorageService.mutationStage_) );
            
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
            /* Check if there were any hints in this message */
            byte[] hintedBytes = message.getHeader(RowMutation.HINT);            
            if ( hintedBytes != null && hintedBytes.length > 0 )
            {
            	EndPoint hint = EndPoint.fromBytes(hintedBytes);
                /* add necessary hints to this mutation */
                try
                {
                	RowMutation hintedMutation = new RowMutation(rm.table(), HintedHandOffManager.key_);
                	hintedMutation.addHints(rm.key() + ":" + hint.getHost());
                	hintedMutation.apply();
                }
                catch ( ColumnFamilyNotDefinedException ex )
                {
                    logger_.debug(LogUtil.throwableToString(ex));
                }
            }
            
            long start = System.currentTimeMillis(); 

            rowMutationCtx.row_.clear();
            rowMutationCtx.row_.key(rm.key());
            rm.apply(rowMutationCtx.row_);
            
            long end = System.currentTimeMillis();                       
            logger_.info("ROW MUTATION APPLY: " + (end - start) + " ms.");
            
            WriteResponse response = new WriteResponse(rm.table(), rm.key(), true);
            Message responseMessage = WriteResponse.makeWriteResponseMessage(message, response);
            logger_.debug("Sending response to " +  message.getFrom() + " for key :" + rm.key());
            MessagingService.getMessagingInstance().sendOneWay(responseMessage, message.getFrom());
        }         
        catch( ColumnFamilyNotDefinedException ex )
        {
            // TODO shouldn't this be checked before it's sent to us?
            logger_.debug(LogUtil.throwableToString(ex));
        }
        catch ( IOException e )
        {
            throw new RuntimeException(e);
        }
    }
}
