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

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.*;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IAsyncResult;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.utils.LogUtil;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocolFactory;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.server.TThreadPoolServer.Options;
import com.facebook.thrift.transport.TServerSocket;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public final class CassandraServer extends FacebookBase implements Cassandra.Iface
{

	private static Logger logger_ = Logger.getLogger(CassandraServer.class);
	/*
	 * Handle to the storage service to interact with the other machines in the
	 * cluster.
	 */
	StorageService storageService_;

	public CassandraServer() throws Throwable
	{
		super("Peerstorage");
		// Create the instance of the storage service
		storageService_ = StorageService.instance();
	}

	/*
	 * The start function initializes the server and starts listening on the specified port
	 */
	public void start() throws Throwable
	{
		//LogUtil.setLogLevel("com.facebook", "DEBUG");
		// Start the storage service
		storageService_.start();
	}

	private Map<EndPoint, Message> createWriteMessages(RowMutationMessage rmMessage, Map<EndPoint, EndPoint> endpointMap) throws IOException
	{
		Map<EndPoint, Message> messageMap = new HashMap<EndPoint, Message>();
		Message message = RowMutationMessage.makeRowMutationMessage(rmMessage);
		
		for (Map.Entry<EndPoint, EndPoint> entry : endpointMap.entrySet())
		{
            EndPoint target = entry.getKey();
            EndPoint hint = entry.getValue();
            if ( !target.equals(hint) )
			{
				Message hintedMessage = RowMutationMessage.makeRowMutationMessage(rmMessage);
				hintedMessage.addHeader(RowMutationMessage.hint_, EndPoint.toBytes(hint) );
				logger_.debug("Sending the hint of " + target.getHost() + " to " + hint.getHost());
				messageMap.put(target, hintedMessage);
			}
			else
			{
				messageMap.put(target, message);
			}
		}
		return messageMap;
	}
	
	private void insert(RowMutation rm)
	{
		// 1. Get the N nodes from storage service where the data needs to be
		// replicated
		// 2. Construct a message for read\write
		// 3. SendRR ( to all the nodes above )
		// 4. Wait for a response from atleast X nodes where X <= N
		// 5. return success

		try
		{
			logger_.debug(" insert");
			Map<EndPoint, EndPoint> endpointMap = storageService_.getNStorageEndPointMap(rm.key());
			// TODO: throw a thrift exception if we do not have N nodes
			RowMutationMessage rmMsg = new RowMutationMessage(rm); 
			/* Create the write messages to be sent */
			Map<EndPoint, Message> messageMap = createWriteMessages(rmMsg, endpointMap);
			for (Map.Entry<EndPoint, Message> entry : messageMap.entrySet())
			{
				MessagingService.getMessagingInstance().sendOneWay(entry.getValue(), entry.getKey());
			}
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return;
	}

   /**
    * Performs the actual reading of a row out of the StorageService, fetching
    * a specific set of column names from a given column family.
    */
    private Row readProtocol(ReadParameters params, StorageService.ConsistencyLevel consistencyLevel) throws Exception
	{
		EndPoint[] endpoints = storageService_.getNStorageEndPoint(params.key);
        boolean foundLocal = Arrays.asList(endpoints).contains(StorageService.getLocalStorageEndPoint());

		if(!foundLocal && consistencyLevel == StorageService.ConsistencyLevel.WEAK)
		{
            EndPoint endPoint = null;
            try {
                endPoint = storageService_.findSuitableEndPoint(params.key);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            Message message = ReadParameters.makeReadMessage(params);
            IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, endPoint);
            Object[] result = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            byte[] body = (byte[]) result[0];
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            ReadResponseMessage responseMessage = ReadResponseMessage.serializer().deserialize(bufIn);
            return responseMessage.row();
        }
		else
		{
			switch ( consistencyLevel )
			{
                case WEAK:
                    return weakReadProtocol(params);
                case STRONG:
                    return strongReadProtocol(params);
                default:
                    throw new UnsupportedOperationException();
			}
		}
	}

    /*
     * This function executes the read protocol.
        // 1. Get the N nodes from storage service where the data needs to be
        // replicated
        // 2. Construct a message for read\write
         * 3. Set one of teh messages to get teh data and teh rest to get teh digest
        // 4. SendRR ( to all the nodes above )
        // 5. Wait for a response from atleast X nodes where X <= N and teh data node
         * 6. If the digest matches return teh data.
         * 7. else carry out read repair by getting data from all the nodes.
        // 5. return success
     */
	private Row strongReadProtocol(ReadParameters params) throws Exception
	{		
        long startTime = System.currentTimeMillis();		
		// TODO: throw a thrift exception if we do not have N nodes

        ReadParameters readMessageDigestOnly = params.copy();
		readMessageDigestOnly.setIsDigestQuery(true);

        Row row = null;
        Message message = ReadParameters.makeReadMessage(params);
        Message messageDigestOnly = ReadParameters.makeReadMessage(readMessageDigestOnly);

        IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
        QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(
                DatabaseDescriptor.getReplicationFactor(),
                readResponseResolver);
        EndPoint dataPoint = storageService_.findSuitableEndPoint(params.key);
        List<EndPoint> endpointList = new ArrayList<EndPoint>( Arrays.asList( storageService_.getNStorageEndPoint(params.key) ) );
        /* Remove the local storage endpoint from the list. */
        endpointList.remove( dataPoint );
        EndPoint[] endPoints = new EndPoint[endpointList.size() + 1];
        Message messages[] = new Message[endpointList.size() + 1];

        // first message is the data Point
        endPoints[0] = dataPoint;
        messages[0] = message;

        for(int i=1; i < endPoints.length ; i++)
        {
            endPoints[i] = endpointList.get(i-1);
            messages[i] = messageDigestOnly;
        }

        try {
            MessagingService.getMessagingInstance().sendRR(messages, endPoints, quorumResponseHandler);

            long startTime2 = System.currentTimeMillis();
            row = quorumResponseHandler.get();
            logger_.info("quorumResponseHandler: " + (System.currentTimeMillis() - startTime2) + " ms.");
        }
        catch (DigestMismatchException ex) {
            IResponseResolver<Row> readResponseResolverRepair = new ReadResponseResolver();
            QuorumResponseHandler<Row> quorumResponseHandlerRepair = new QuorumResponseHandler<Row>(
                    DatabaseDescriptor.getReplicationFactor(),
                    readResponseResolverRepair);
            params.setIsDigestQuery(false);
            logger_.info("DigestMismatchException: " + params.key);
            Message messageRepair = ReadParameters.makeReadMessage(params);
            MessagingService.getMessagingInstance().sendRR(messageRepair, endPoints,
                                                           quorumResponseHandlerRepair);
            try {
                row = quorumResponseHandlerRepair.get();
            }
            catch (DigestMismatchException dex) {
                logger_.error(LogUtil.throwableToString(dex));
            }
        }

        logger_.info("readProtocol: " + (System.currentTimeMillis() - startTime) + " ms.");
		return row;
	}

    /*
    * This function executes the read protocol locally and should be used only if consistency is not a concern.
    * Read the data from the local disk and return if the row is NOT NULL. If the data is NULL do the read from
    * one of the other replicas (in the same data center if possible) till we get the data. In the event we get
    * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
    */
	private Row weakReadProtocol(ReadParameters params) throws Exception
	{		
		long startTime = System.currentTimeMillis();
		List<EndPoint> endpoints = storageService_.getNLiveStorageEndPoint(params.key);
		/* Remove the local storage endpoint from the list. */ 
		endpoints.remove( StorageService.getLocalStorageEndPoint() );
		// TODO: throw a thrift exception if we do not have N nodes
		
		Table table = Table.open( DatabaseDescriptor.getTables().get(0) );
		Row row = params.getRow(table);
		logger_.info("Local Read Protocol: " + (System.currentTimeMillis() - startTime) + " ms.");

		/*
		 * Do the consistency checks in the background and return the
		 * non NULL row.
		 */
		if ( endpoints.size() > 0 )
			StorageService.instance().doConsistencyCheck(row, endpoints, params);
		return row;
	}

    public  ArrayList<column_t> get_columns_since(String tablename, String key, String columnFamily_column, long timeStamp) throws TException
	{
		ArrayList<column_t> retlist = new ArrayList<column_t>();
        long startTime = System.currentTimeMillis();
		
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 1 )
	        	return retlist;
	        
	        Row row = readProtocol(new ReadParameters(tablename, key, columnFamily_column, timeStamp), StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception 
				return retlist;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily " + columnFamily_column + " map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception 
				return retlist;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily_column + " is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return retlist;
			}
			Collection<IColumn> columns = null;
			if( values.length > 1 )
			{
				// this is the super column case 
				IColumn column = cfamily.getColumn(values[1]);
				if(column != null)
					columns = column.getSubColumns();
			}
			else
			{
				columns = cfamily.getAllColumns();
			}
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception 
				return retlist;
			}
			
			for(IColumn column : columns)
			{
				column_t thrift_column = new column_t();
				thrift_column.columnName = column.name();
				thrift_column.value = new String(column.value()); // This needs to be Utf8ed
				thrift_column.timestamp = column.timestamp();
				retlist.add(thrift_column);
			}
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		
        logger_.info("get_slice2: " + (System.currentTimeMillis() - startTime)
                + " ms.");
		
		return retlist;
	}
	
	
	
    public ArrayList<column_t> get_slice(String tablename, String key, String columnFamily_column, int start, int count) throws TException
	{
		ArrayList<column_t> retlist = new ArrayList<column_t>();
        long startTime = System.currentTimeMillis();
		
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 1 )
	        	return retlist;
	        
	        Row row = readProtocol(new ReadParameters(tablename, key, columnFamily_column, start, count), StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception 
				return retlist;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily " + columnFamily_column + " map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception 
				return retlist;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily_column + " is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return retlist;
			}
			Collection<IColumn> columns = null;
			if( values.length > 1 )
			{
				// this is the super column case 
				IColumn column = cfamily.getColumn(values[1]);
				if(column != null)
					columns = column.getSubColumns();
			}
			else
			{
				columns = cfamily.getAllColumns();
			}
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception 
				return retlist;
			}
			
			for(IColumn column : columns)
			{
				column_t thrift_column = new column_t();
				thrift_column.columnName = column.name();
				thrift_column.value = new String(column.value()); // This needs to be Utf8ed
				thrift_column.timestamp = column.timestamp();
				retlist.add(thrift_column);
			}
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		
        logger_.info("get_slice2: " + (System.currentTimeMillis() - startTime)
                + " ms.");
		
		return retlist;
	}
    
    public column_t get_column(String tablename, String key, String columnFamily_column) throws TException
    {
		column_t ret = null;
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 2 )
	        	return ret;
	        Row row = readProtocol(new ReadParameters(tablename, key, columnFamily_column), StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception 
				return ret;
			}
			
			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception 
				return ret;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return ret;
			}
			Collection<IColumn> columns = null;
			if( values.length > 2 )
			{
				// this is the super column case 
				IColumn column = cfamily.getColumn(values[1]);
				if(column != null)
					columns = column.getSubColumns();
			}
			else
			{
				columns = cfamily.getAllColumns();
			}
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception 
				return ret;
			}
			ret = new column_t();
			for(IColumn column : columns)
			{
				ret.columnName = column.name();
				ret.value = new String(column.value());
				ret.timestamp = column.timestamp();
			}
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return ret;
    	
    }
    

    public int get_column_count(String tablename, String key, String columnFamily_column)
	{
    	int count = -1;
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 1 )
	        	return -1;
	        Row row = readProtocol(new ReadParameters(tablename, key, columnFamily_column), StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception 
				return count;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception 
				return count;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return count;
			}
			Collection<IColumn> columns = null;
			if( values.length > 1 )
			{
				// this is the super column case 
				IColumn column = cfamily.getColumn(values[1]);
				if(column != null)
					columns = column.getSubColumns();
			}
			else
			{
				columns = cfamily.getAllColumns();
			}
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception 
				return count;
			}
			count = columns.size();
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return count;

	}

    public void insert(String tablename, String key, String columnFamily_column, String cellData, long timestamp)
	{

		try
		{
			RowMutation rm = new RowMutation(tablename, key.trim());
			rm.add(columnFamily_column, cellData.getBytes(), timestamp);
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return;
	}

    public boolean batch_insert_blocking(batch_mutation_t batchMutation)
    {
        logger_.warn("batch_insert_blocking");
    	boolean result = false;
		try
		{
            RowMutation rm = RowMutation.getRowMutation(batchMutation);
            RowMutationMessage rmMsg = new RowMutationMessage(rm);
            Message message = RowMutationMessage.makeRowMutationMessage(rmMsg);
            
            IResponseResolver<Boolean> writeResponseResolver = new WriteResponseResolver();
            QuorumResponseHandler<Boolean> quorumResponseHandler = new QuorumResponseHandler<Boolean>(
                    DatabaseDescriptor.getReplicationFactor(),
                    writeResponseResolver);
            EndPoint[] endpoints = storageService_.getNStorageEndPoint(batchMutation.key);
            // TODO: throw a thrift exception if we do not have N nodes

			MessagingService.getMessagingInstance().sendRR(message, endpoints,
					quorumResponseHandler);
			logger_.debug(" Calling quorum response handler's get");
			result = quorumResponseHandler.get(); 
                       
			// TODO: if the result is false that means the writes to all the
			// servers failed hence we need to throw an exception or return an
			// error back to the client so that it can take appropriate action.
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return result;
    	
    }

    public void batch_insert(batch_mutation_t batchMutation)
	{
        logger_.debug("batch_insert");

		try
		{
            RowMutation rm = RowMutation.getRowMutation(batchMutation);
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return;
	}

    public void remove(String tablename, String key, String columnFamily_column, long timestamp)
	{
		try
		{
			RowMutation rm = new RowMutation(tablename, key.trim());
			rm.delete(columnFamily_column, timestamp);
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return;
	}

    public ArrayList<superColumn_t> get_slice_super(String tablename, String key, String columnFamily_superColumnName, int start, int count)
    {
		ArrayList<superColumn_t> retlist = new ArrayList<superColumn_t>();
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_superColumnName);
	        // check for  values 
	        if( values.length < 1 )
	        	return retlist;
	        Row row = readProtocol(new ReadParameters(tablename, key, columnFamily_superColumnName, start, count), StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception 
				return retlist;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception 
				return retlist;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return retlist;
			}
			Collection<IColumn> columns = cfamily.getAllColumns();
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception 
				return retlist;
			}
			
			for(IColumn column : columns)
			{
				superColumn_t thrift_superColumn = new superColumn_t();
				thrift_superColumn.name = column.name();
				Collection<IColumn> subColumns = column.getSubColumns();
				if(subColumns.size() != 0 )
				{
					thrift_superColumn.columns = new ArrayList<column_t>();
					for( IColumn subColumn : subColumns )
					{
						column_t thrift_column = new column_t();
						thrift_column.columnName = subColumn.name();
						thrift_column.value = new String(subColumn.value());
						thrift_column.timestamp = subColumn.timestamp();
						thrift_superColumn.columns.add(thrift_column);
					}
                    retlist.add(thrift_superColumn);
				}
			}
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return retlist;
    	
    }
    
    public superColumn_t get_superColumn(String tablename, String key, String columnFamily_column)
    {
    	superColumn_t ret = null;
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 2 )
	        	return ret;

	        Row row = readProtocol(new ReadParameters(tablename, key, columnFamily_column), StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception 
				return ret;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception 
				return ret;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return ret;
			}
			Collection<IColumn> columns = cfamily.getAllColumns();
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception 
				return ret;
			}
			
			for(IColumn column : columns)
			{
				ret = new superColumn_t();
				ret.name = column.name();
				Collection<IColumn> subColumns = column.getSubColumns();
				if(subColumns.size() != 0 )
				{
					ret.columns = new ArrayList<column_t>();
					for(IColumn subColumn : subColumns)
					{
						column_t thrift_column = new column_t();
						thrift_column.columnName = subColumn.name();
						thrift_column.value = new String(subColumn.value());
						thrift_column.timestamp = subColumn.timestamp();
						ret.columns.add(thrift_column);
					}
				}
			}
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return ret;
    	
    }
    
    public boolean batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper)
    {
        logger_.debug("batch_insert_SuperColumn_blocking");
    	boolean result = false;
		try
		{
            RowMutation rm = RowMutation.getRowMutation(batchMutationSuper);
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return result;
    	
    }

    public void batch_insert_superColumn(batch_mutation_super_t batchMutationSuper)
    {
        logger_.debug("batch_insert_SuperColumn_blocking");
		try
		{
            RowMutation rm = RowMutation.getRowMutation(batchMutationSuper);
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.error( LogUtil.throwableToString(e) );
		}
		return;
    }
    
    
	public String getVersion()
	{
		return "1";
	}

	public int getStatus()
	{
		return fb_status.ALIVE;
	}

	public String getStatusDetails()
	{
		return null;
	}

	public static void main(String[] args) throws Throwable
	{
		int port = 9160;		
		try
		{
			CassandraServer peerStorageServer = new CassandraServer();
			peerStorageServer.start();
			Cassandra.Processor processor = new Cassandra.Processor(
					peerStorageServer);
			// Transport
			TServerSocket tServerSocket =  new TServerSocket(port);
			 // Protocol factory
			TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory();
			 // ThreadPool Server
			Options options = new Options();
			options.minWorkerThreads = 64;
			TThreadPoolServer serverEngine = new TThreadPoolServer(processor, tServerSocket, tProtocolFactory);
			serverEngine.serve();

		}
		catch (Throwable x)
		{
            logger_.error("Fatal error; exiting", x);
			System.exit(1);
		}

	}

}
