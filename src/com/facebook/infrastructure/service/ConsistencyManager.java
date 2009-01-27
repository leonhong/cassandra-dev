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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.facebook.infrastructure.concurrent.DebuggableScheduledThreadPoolExecutor;
import com.facebook.infrastructure.concurrent.ThreadFactoryImpl;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.ReadMessage;
import com.facebook.infrastructure.db.ReadResponseMessage;
import com.facebook.infrastructure.db.Row;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IAsyncCallback;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.utils.Cachetable;
import com.facebook.infrastructure.utils.ICacheExpungeHook;
import com.facebook.infrastructure.utils.ICachetable;
import com.facebook.infrastructure.utils.LogUtil;

class ConsistencyManager implements Runnable
{
	private static Logger logger_ = Logger.getLogger(ConsistencyManager.class);
	
	class DigestResponseHandler implements IAsyncCallback
	{
		List<Message> responses_ = new ArrayList<Message>();
		
		public void response(Message msg)
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
			for( Message response : responses_ )
			{
				byte[] body = (byte[])response.getMessageBody()[0];            
	            bufIn.reset(body, body.length);
	            try
	            {	               
	                ReadResponseMessage result = ReadResponseMessage.serializer().deserialize(bufIn);  
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
			ReadMessage readMessage = new ReadMessage(table, row_.key(), columnFamily_);
            Message message = ReadMessage.makeReadMessage(readMessage);
			MessagingService.getMessagingInstance().sendRR(message, replicas_.toArray( new EndPoint[0] ), responseHandler);			
		}
	}
	
	class DataRepairHandler implements IAsyncCallback, ICacheExpungeHook<String, String>
	{
		private List<Message> responses_ = new ArrayList<Message>();
		private IResponseResolver<Row> readResponseResolver_;
		private int majority_;
		
		DataRepairHandler(int responseCount, IResponseResolver<Row> readResponseResolver)
		{
			readResponseResolver_ = readResponseResolver;
			majority_ = (responseCount >> 1) + 1;  
		}
		
		public void response(Message message)
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
				readResponseResolver_.resolve(new ArrayList<Message>(responses_));
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
		ReadMessage readMessageDigestOnly = null;
		if(columnNames_.size() == 0)
		{
			if( start_ >= 0 && count_ < Integer.MAX_VALUE)
			{
				readMessageDigestOnly = new ReadMessage(table, row_.key(), columnFamily_, start_, count_);
			}
			else if(sinceTimestamp_ > 0)
			{
				readMessageDigestOnly = new ReadMessage(table, row_.key(), columnFamily_, sinceTimestamp_);
			}
			else
			{
				readMessageDigestOnly = new ReadMessage(table, row_.key(), columnFamily_);
			}
		}
		else
		{
			readMessageDigestOnly = new ReadMessage(table, row_.key(), columnFamily_, columnNames_);
			
		}
		readMessageDigestOnly.setIsDigestQuery(true);
		try
		{
			Message messageDigestOnly = ReadMessage.makeReadMessage(readMessageDigestOnly);
			IAsyncCallback digestResponseHandler = new DigestResponseHandler();
			MessagingService.getMessagingInstance().sendRR(messageDigestOnly, replicas_.toArray(new EndPoint[0]), digestResponseHandler);
		}
		catch ( IOException ex )
		{
			logger_.info(LogUtil.throwableToString(ex));
		}
	}
}
