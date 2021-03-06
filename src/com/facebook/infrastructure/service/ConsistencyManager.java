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

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.ReadParameters;
import com.facebook.infrastructure.db.ReadResponse;
import com.facebook.infrastructure.db.Row;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.net.*;
import com.facebook.infrastructure.utils.Cachetable;
import com.facebook.infrastructure.utils.ICacheExpungeHook;
import com.facebook.infrastructure.utils.ICachetable;
import com.facebook.infrastructure.utils.LogUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ConsistencyManager implements Runnable
{
	private static Logger logger_ = Logger.getLogger(ConsistencyManager.class);
	
	class DigestResponseHandler implements IAsyncCallback
	{
		List<Message<byte[]>> responses_ = new ArrayList<Message<byte[]>>();
		
		public void response(Message<byte[]> msg)
		{
			logger_.debug("Received reponse : " + msg.toString());
			responses_.add(msg);
			if ( responses_.size() == ConsistencyManager.this.replicas_.size() )
				handleDigestResponses();
		}
		
		private void handleDigestResponses()
		{
			DataInputBuffer bufIn = new DataInputBuffer();
			logger_.debug("Handle Digest reponses");
			for( Message<byte[]> response : responses_ )
			{
				byte[] body = response.getMessageBody();            
	            bufIn.reset(body, body.length);
	            try
	            {	               
	                ReadResponse result = ReadResponse.serializer().deserialize(bufIn);
	                byte[] digest = result.digest();
	                if( !Arrays.equals(row_.digest(), digest) )
					{
	                	doReadRepair();
	                	break;
					}
	            }
	            catch( IOException ex )
	            {
	            	logger_.info(LogUtil.throwableToString(ex));
	            }
			}
		}
		
		private void doReadRepair() throws IOException
		{
			IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
            /* Add the local storage endpoint to the replicas_ list */
            replicas_.add(StorageService.getLocalStorageEndPoint());
			IAsyncCallback responseHandler = new DataRepairHandler(ConsistencyManager.this.replicas_.size(), readResponseResolver);	
			String table = DatabaseDescriptor.getTables().get(0);
			ReadParameters readMessage = new ReadParameters(table, row_.key(), columnFamily_);
            Message message = ReadParameters.makeReadMessage(readMessage);
			MessagingService.getMessagingInstance().sendRR(message, replicas_.toArray( new EndPoint[0] ), responseHandler);			
		}
	}
	
	class DataRepairHandler implements IAsyncCallback, ICacheExpungeHook<String, String>
	{
		private List<Message<byte[]>> responses_ = new ArrayList<Message<byte[]>>();
		private IResponseResolver<Row> readResponseResolver_;
		private int majority_;
		
		DataRepairHandler(int responseCount, IResponseResolver<Row> readResponseResolver)
		{
			readResponseResolver_ = readResponseResolver;
			majority_ = (responseCount >> 1) + 1;  
		}
		
		public void response(Message<byte[]> message)
		{
			logger_.debug("Received responses in DataRepairHandler : " + message.toString());
			responses_.add(message);
			if ( responses_.size() == majority_ )
			{
				String messageId = message.getMessageId();
				readRepairTable_.put(messageId, messageId, this);
				// handleResponses();
			}
		}
		
		public void callMe(String key, String value)
		{
			handleResponses();
		}
		
		private void handleResponses()
		{
			try
			{
				readResponseResolver_.resolve(new ArrayList<Message<byte[]>>(responses_));
			}
			catch ( DigestMismatchException ex )
			{
				logger_.info("We should not be coming here under any circumstances ...");
				logger_.info(LogUtil.throwableToString(ex));
			}
		}
	}
	private static long scheduledTimeMillis_ = 600;
	private static ICachetable<String, String> readRepairTable_ = new Cachetable<String, String>(scheduledTimeMillis_);
	private Row row_;
	protected List<EndPoint> replicas_;
	private String columnFamily_;
	private int start_;
	private int count_;
	private long sinceTimestamp_;
	private List<String> columnNames_ = new ArrayList<String>();

    public ConsistencyManager(Row row_, List<EndPoint> replicas_, String columnFamily_, int start_, int count_, long sinceTimestamp_, List<String> columnNames_) {
        this.row_ = row_;
        this.replicas_ = replicas_;
        this.columnFamily_ = columnFamily_;
        this.start_ = start_;
        this.count_ = count_;
        this.sinceTimestamp_ = sinceTimestamp_;
        this.columnNames_ = columnNames_;
    }

    ConsistencyManager(Row row, List<EndPoint> replicas, String columnFamily, List<String> columns)
	{
		row_ = row;
		replicas_ = replicas;
		columnFamily_ = columnFamily;
		columnNames_ = columns;
	}
	
	ConsistencyManager(Row row, List<EndPoint> replicas, String columnFamily, int start, int count)
	{
		row_ = row;
		replicas_ = replicas;
		columnFamily_ = columnFamily;
		start_ = start;
		count_ = count;
	}
	
	ConsistencyManager(Row row, List<EndPoint> replicas, String columnFamily, long sinceTimestamp)
	{
		row_ = row;
		replicas_ = replicas;
		columnFamily_ = columnFamily;
		sinceTimestamp_ = sinceTimestamp;
	}

	public void run()
	{
		logger_.debug(" Run the consistency checks for " + columnFamily_);
		String table = DatabaseDescriptor.getTables().get(0);
		ReadParameters readMessageDigestOnly = null;
		if(columnNames_.size() == 0)
		{
			if( start_ >= 0 && count_ < Integer.MAX_VALUE)
			{
				readMessageDigestOnly = new ReadParameters(table, row_.key(), columnFamily_, start_, count_);
			}
			else if(sinceTimestamp_ > 0)
			{
				readMessageDigestOnly = new ReadParameters(table, row_.key(), columnFamily_, sinceTimestamp_);
			}
			else
			{
				readMessageDigestOnly = new ReadParameters(table, row_.key(), columnFamily_);
			}
		}
		else
		{
			readMessageDigestOnly = new ReadParameters(table, row_.key(), columnFamily_, columnNames_);
			
		}
		readMessageDigestOnly.setIsDigestQuery(true);
		try
		{
			Message messageDigestOnly = ReadParameters.makeReadMessage(readMessageDigestOnly);
			IAsyncCallback digestResponseHandler = new DigestResponseHandler();
			MessagingService.getMessagingInstance().sendRR(messageDigestOnly, replicas_.toArray(new EndPoint[0]), digestResponseHandler);
		}
		catch ( IOException ex )
		{
			logger_.info(LogUtil.throwableToString(ex));
		}
	}
}
