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

import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.DataOutputBuffer;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.LogUtil;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ReadVerbHandler implements IVerbHandler<byte[]>
{
    private static class ReadContext
    {
        protected DataInputBuffer bufIn_ = new DataInputBuffer();
        protected DataOutputBuffer bufOut_ = new DataOutputBuffer();
    }

    private static Logger logger_ = Logger.getLogger( ReadVerbHandler.class );
    /* We use this so that we can reuse the same row mutation context for the mutation. */
    private static ThreadLocal<ReadContext> tls_ = new InheritableThreadLocal<ReadContext>();

    public void doVerb(Message<byte[]> message)
    {
        byte[] body = message.getMessageBody();
        /* Obtain a Read Context from TLS */
        ReadContext readCtx = tls_.get();
        if ( readCtx == null )
        {
            readCtx = new ReadContext();
            tls_.set(readCtx);
        }
        readCtx.bufIn_.reset(body, body.length);

        try
        {
            ReadParameters readMessage = ReadParameters.serializer().deserialize(readCtx.bufIn_);
            Table table = Table.open(readMessage.table);
            Row row = null;
            long start = System.currentTimeMillis();
            if( readMessage.columnFamily_column == null )
            	row = table.get(readMessage.key);
            else
            {
            	if(readMessage.getColumnNames().size() == 0)
            	{
	            	if(readMessage.count > 0 && readMessage.start >= 0)
	            		row = table.getRow(readMessage.key, readMessage.columnFamily_column, readMessage.start, readMessage.count);
	            	else
	            		row = table.getRow(readMessage.key, readMessage.columnFamily_column);
            	}
            	else
            	{
            		row = table.getRow(readMessage.key, readMessage.columnFamily_column, readMessage.getColumnNames());
            	}
            }
            long rowfetchTime = System.currentTimeMillis();
            ReadResponse readResponse = null;
            if(readMessage.isDigestQuery())
            {
                readResponse = new ReadResponse(table.getTableName(), row.digest());
            }
            else
            {
                readResponse = new ReadResponse(table.getTableName(), row);
            }
            readResponse.setIsDigestQuery(readMessage.isDigestQuery());
            /* serialize the ReadResponse. */
            readCtx.bufOut_.reset();

            ReadResponse.serializer().serialize(readResponse, readCtx.bufOut_);
            byte[] bytes = new byte[readCtx.bufOut_.getLength()];
            System.arraycopy(readCtx.bufOut_.getData(), 0, bytes, 0, bytes.length);
            Message response = message.getReply( StorageService.getLocalStorageEndPoint(), bytes );
            MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());

            logger_.debug("RVH read " + row + " in " + (rowfetchTime - start) + "ms, " + (System.currentTimeMillis() - start) + "ms total");
        }
        catch ( IOException ex)
        {
            logger_.info( LogUtil.throwableToString(ex) );
        }
        catch ( ColumnFamilyNotDefinedException ex)
        {
            logger_.info( LogUtil.throwableToString(ex) );
        }
    }
}
