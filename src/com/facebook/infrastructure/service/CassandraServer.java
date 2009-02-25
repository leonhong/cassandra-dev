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
import com.facebook.infrastructure.io.DataOutputBuffer;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IAsyncResult;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

	public CassandraServer()
	{
		super("Peerstorage");
		// Create the instance of the storage service
		storageService_ = StorageService.instance();
	}

	/*
	 * The start function initializes the server and starts listening on the specified port
	 */
	public void start() throws IOException {
		//LogUtil.setLogLevel("com.facebook", "DEBUG");
		// Start the storage service
		storageService_.start();
	}

	private Map<EndPoint, Message> createWriteMessages(RowMutation rm, Map<EndPoint, EndPoint> endpointMap) throws IOException
	{
		Map<EndPoint, Message> messageMap = new HashMap<EndPoint, Message>();
		Message message = rm.makeRowMutationMessage();
		
		for (Map.Entry<EndPoint, EndPoint> entry : endpointMap.entrySet())
		{
            EndPoint target = entry.getKey();
            EndPoint hint = entry.getValue();
            if ( !target.equals(hint) )
			{
				Message hintedMessage = rm.makeRowMutationMessage();
				hintedMessage.addHeader(rm.HINT, EndPoint.toBytes(hint) );
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
        assert rm.key() != null;
        
		// 1. Get the N nodes from storage service where the data needs to be
		// replicated
		// 2. Construct a message for read\write
		// 3. SendRR ( to all the nodes above )
		// 4. Wait for a response from atleast X nodes where X <= N
		// 5. return success
		try
		{
			Map<EndPoint, EndPoint> endpointMap = storageService_.getNStorageEndPointMap(rm.key());
			// TODO: throw a thrift exception if we do not have N nodes
			/* Create the write messages to be sent */
			Map<EndPoint, Message> messageMap = createWriteMessages(rm, endpointMap);
            logger_.debug("insert writing to [" + StringUtils.join(messageMap.keySet(), ", ") + "]");
			for (Map.Entry<EndPoint, Message> entry : messageMap.entrySet())
			{
				MessagingService.getMessagingInstance().sendOneWay(entry.getValue(), entry.getKey());
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

   /**
    * Performs the actual reading of a row out of the StorageService, fetching
    * a specific set of column names from a given column family.
    */
    private Row readProtocol(ReadParameters params, StorageService.ConsistencyLevel consistencyLevel)
    throws IOException, ColumnFamilyNotDefinedException
    {
        assert params.key != null;
        long startTime = System.currentTimeMillis();
        Row row = null;
		EndPoint[] endpoints = storageService_.getNStorageEndPoint(params.key);

        if (consistencyLevel == StorageService.ConsistencyLevel.WEAK) {
            boolean foundLocal = Arrays.asList(endpoints).contains(StorageService.getLocalStorageEndPoint());
            if (foundLocal) {
                row = weakReadLocal(params);
            } else {
                row = weakReadRemote(params);
            }
        } else {
            assert consistencyLevel == StorageService.ConsistencyLevel.STRONG;
            row = strongRead(params);
        }

        logger_.debug("Finished reading " + row + " in " + (System.currentTimeMillis() - startTime) + " ms.");
        return row;
	}

    private Row weakReadRemote(ReadParameters params) throws IOException {
        EndPoint endPoint = null;
        try {
            endPoint = storageService_.findSuitableEndPoint(params.key);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        logger_.debug("weakreadremote reading " + params + " from " + endPoint);
        Message message = ReadParameters.makeReadMessage(params);
        IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, endPoint);
        byte[] body;
        try {
            body = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
            // TODO retry to a different endpoint
        }
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(body, body.length);
        ReadResponse response = ReadResponse.serializer().deserialize(bufIn);
        return response.row();
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
	private Row strongRead(ReadParameters params) throws IOException {
		// TODO: throw a thrift exception if we do not have N nodes
        // TODO: how can we throw ColumnFamilyNotDefined here?

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
        logger_.debug("strongread reading " + params + " from " + StringUtils.join(endPoints, ", "));

        try {
            MessagingService.getMessagingInstance().sendRR(messages, endPoints, quorumResponseHandler);

            long startTime2 = System.currentTimeMillis();
            try {
                row = quorumResponseHandler.get();
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger_.debug("quorumResponseHandler: " + (System.currentTimeMillis() - startTime2) + " ms.");
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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

		return row;
	}

    /*
    * This function executes the read protocol locally and should be used only if consistency is not a concern.
    * Read the data from the local disk and return if the row is NOT NULL. If the data is NULL do the read from
    * one of the other replicas (in the same data center if possible) till we get the data. In the event we get
    * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
    */
	private Row weakReadLocal(ReadParameters params) throws IOException, ColumnFamilyNotDefinedException {
        logger_.debug("weakreadlocal for " + params);
		List<EndPoint> endpoints = storageService_.getNLiveStorageEndPoint(params.key);
		/* Remove the local storage endpoint from the list. */ 
		endpoints.remove( StorageService.getLocalStorageEndPoint() );
		// TODO: throw a thrift exception if we do not have N nodes
		
		Table table = Table.open( DatabaseDescriptor.getTables().get(0) );
		Row row = params.getRow(table);

		/*
		 * Do the consistency checks in the background and return the
		 * non NULL row.
		 */
		if ( endpoints.size() > 0 )
			StorageService.instance().doConsistencyCheck(row, endpoints, params);
		return row;
	}

    private Collection<IColumn> getColumns(ReadParameters params, StorageService.ConsistencyLevel consistency) throws ColumnFamilyNotDefinedException {
        ColumnFamily cf = getCF(params, consistency);
        String[] values = RowMutation.getColumnAndColumnFamily(params.columnFamily_column);
        if (cf == null) {
            return Arrays.asList(new IColumn[0]);
        }

        if (values.length == 1) {
            return cf.getAllColumns();
        }

        IColumn column = cf.getColumn(values[1]);
        if (column == null) {
            return Arrays.asList(new IColumn[0]);
        }
        return column.getSubColumns();
    }

    /**
     * Gets the ColumnFamily object for the given table, key, and cf.
     * Returns null if column family is defined, but has no columns for the given key.
     */
    private ColumnFamily getCF(ReadParameters params, StorageService.ConsistencyLevel consistency) throws ColumnFamilyNotDefinedException {
        Row row;
        try {
            row = readProtocol(params, consistency);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assert row != null; // should be empty row if key not present

        return row.getColumnFamily(params.columnFamily_column);
    }

    private ArrayList<column_t> getThriftColumns(ReadParameters params, StorageService.ConsistencyLevel consistency) throws ColumnFamilyNotDefinedException {
        ArrayList<column_t> retlist = new ArrayList<column_t>();
        for (IColumn column : getColumns(params, consistency)) {
            retlist.add(makeThriftColumn(column));
        }
        return retlist;
    }

    /**
     * Convert a Java IColumn into a column_t suitable for returning
     */
    private column_t makeThriftColumn(IColumn column) {
        column_t thrift_column = new column_t();
        thrift_column.columnName = column.name();
        thrift_column.value = new String(column.value()); // This needs to be Utf8ed
        thrift_column.timestamp = column.timestamp();
        return thrift_column;
    }

    private superColumn_t makeThriftSuperColumn(IColumn column) {
        superColumn_t ret = new superColumn_t();
        ret.name = column.name();
        Collection<IColumn> subColumns = column.getSubColumns();
        ret.columns = new ArrayList<column_t>();
        for (IColumn subColumn : subColumns) {
            ret.columns.add(makeThriftColumn(subColumn));
        }
        return ret;
    }

    public ArrayList<column_t> get_columns_since(String tablename, String key, String columnFamily_column, long timeStamp) throws InvalidRequestException {
        logger_.debug("get_columns_since");
        if (columnFamily_column.isEmpty()) {
            throw new InvalidRequestException("Column family required");
        }
        return getThriftColumns(new ReadParameters(tablename, key, columnFamily_column, timeStamp), StorageService.ConsistencyLevel.WEAK);
	}

    public ArrayList<column_t> get_slice(String tablename, String key, String columnFamily_column, int start, int count) throws InvalidRequestException {
        logger_.debug("get_slice");
        if (columnFamily_column.isEmpty()) {
            throw new InvalidRequestException("Column family required");
        }
        return getThriftColumns(new ReadParameters(tablename, key, columnFamily_column, start, count), StorageService.ConsistencyLevel.WEAK);
	}

    public ArrayList<column_t> get_slice_strong(String tablename, String key, String columnFamily_column, int start, int count) throws InvalidRequestException {
        logger_.debug("get_slice_strong");
        if (columnFamily_column.isEmpty()) {
            throw new InvalidRequestException("Column family required");
        }
        return getThriftColumns(new ReadParameters(tablename, key, columnFamily_column, start, count), StorageService.ConsistencyLevel.STRONG);
	}

    public column_t get_column(String tablename, String key, String columnFamily_column) throws InvalidRequestException, NotFoundException {
        logger_.debug("get_column");
        // Check format of column argument
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);

        if (values.length < 2) {
            throw new InvalidRequestException(
                    "get_column expects either 'columnFamily:column' or 'columnFamily:superCol:col'");
        }

        String columnFamilyName = values[0];
        String columnName = values[1];

        ColumnFamily cf = getCF(new ReadParameters(tablename, key, columnFamilyName, Arrays.asList(new String[] {columnName})),
                                StorageService.ConsistencyLevel.WEAK);

        IColumn column = cf == null ? null : cf.getColumn(columnName);

        // Handle supercolumn fetches
        if (column != null && values.length == 3) {
            // They want a column within a supercolumn
            try {
                SuperColumn sc = (SuperColumn) column;
                column = sc.getSubColumn(values[2]);
            }
            catch (ClassCastException cce) {
                throw new InvalidRequestException("Column " + values[1] + " is not a supercolumn.");
            }
        }
        if (column == null) {
            throw new NotFoundException();
        }
        
        return makeThriftColumn(column);
    }

    public int get_column_count(String tablename, String key, String columnFamily_column)
            throws InvalidRequestException {
        logger_.debug("get_column_count");
        if (columnFamily_column.isEmpty()) {
            throw new InvalidRequestException("Column family required");
        }
        Collection<IColumn> columns = getColumns(new ReadParameters(tablename, key, columnFamily_column), StorageService.ConsistencyLevel.WEAK);
        return columns.size();
    }

    public ArrayList<superColumn_t> get_slice_super(String tablename, String key, String columnFamily_superColumnName, int start, int count)
            throws InvalidRequestException {
        logger_.debug("get_slice_super");
        if (columnFamily_superColumnName.isEmpty()) {
            throw new InvalidRequestException("Column family required");
        }

        Collection<IColumn> columns = getColumns(new ReadParameters(tablename, key, columnFamily_superColumnName, start, count), StorageService.ConsistencyLevel.WEAK);
        ArrayList<superColumn_t> retlist = new ArrayList<superColumn_t>();
        for (IColumn column : columns) {
            if (column instanceof Column || column.getSubColumns().size() > 0) {
                retlist.add(makeThriftSuperColumn(column));
            }
        }
        return retlist;
    }

    public superColumn_t get_superColumn(String tablename, String key, String columnFamily_column)
            throws InvalidRequestException, NotFoundException {
        logger_.debug("get_superColumn");
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
        if (values.length != 2) {
            throw new InvalidRequestException("get_superColumn expects column of form cfamily:supercol");
        }

        ColumnFamily cf = getCF(new ReadParameters(tablename, key, values[0]),
                                StorageService.ConsistencyLevel.WEAK);
        IColumn column = cf == null ? null : cf.getColumn(values[1]);
        if (column == null) {
            throw new NotFoundException();
        }

        return makeThriftSuperColumn(column);
    }

    public List<String> get_range(String tablename, final String startkey) {
        logger_.debug("get_range");
        // send the request
        // for now we ignore tablename like 90% of the rest of cassandra
        Message message;
        DataOutputBuffer dob = new DataOutputBuffer();
        try {
            dob.writeUTF(startkey);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] messageBody = Arrays.copyOf(dob.getData(), dob.getLength());
        message = new Message(StorageService.getLocalStorageEndPoint(),
                        StorageService.readStage_,
                        StorageService.rangeVerbHandler_,
                        messageBody);
        EndPoint endPoint;
        try {
            endPoint = storageService_.findSuitableEndPoint(startkey);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, endPoint);

        // read response
        byte[] responseBody;
        try {
            responseBody = iar.get(2 * DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(responseBody, responseBody.length);

        // turn into List
        List<String> keys = new ArrayList<String>();
        while (bufIn.getPosition() < responseBody.length) {
            try {
                keys.add(bufIn.readUTF());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return keys;
    }

    public boolean insert(String tablename, String key, String columnFamily_column, String cellData, long timestamp, int block_for)
	{
        logger_.debug("insert");
        RowMutation rm = new RowMutation(tablename, key.trim());
        rm.add(columnFamily_column, cellData.getBytes(), timestamp);
        if (block_for > 0) {
            return insertBlocking(rm);
        } else {
            insert(rm);
            return true;
        }
	}

    private boolean insertBlocking(RowMutation rm) {
        assert rm.key() != null;

        try
		{
            Message message = rm.makeRowMutationMessage();

            IResponseResolver<Boolean> writeResponseResolver = new WriteResponseResolver();
            QuorumResponseHandler<Boolean> quorumResponseHandler = new QuorumResponseHandler<Boolean>(
                    DatabaseDescriptor.getReplicationFactor(),
                    writeResponseResolver);
            EndPoint[] endpoints = storageService_.getNStorageEndPoint(rm.key());
            logger_.debug("insertBlocking writing to [" + StringUtils.join(endpoints, ", ") + "]");
            // TODO: throw a thrift exception if we do not have N nodes

            MessagingService.getMessagingInstance().sendRR(message, endpoints, quorumResponseHandler);
            return quorumResponseHandler.get();

            // TODO: if the result is false that means the writes to all the
            // servers failed hence we need to throw an exception or return an
            // error back to the client so that it can take appropriate action.
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean batch_insert(batch_mutation_t batchMutation, int block_for)
	{
        logger_.debug("batch_insert");
        RowMutation rm = RowMutation.getRowMutation(batchMutation);
        if (block_for > 0) {
            return insertBlocking(rm);
        } else {
            insert(rm);
            return true;
        }
	}

    public boolean remove(String tablename, String key, String columnFamily_column, long timestamp, int block_for)
	{
        logger_.debug("remove");
        RowMutation rm = new RowMutation(tablename, key.trim());
        rm.delete(columnFamily_column, timestamp);
        if (block_for > 0) {
            return insertBlocking(rm);
        } else {
            insert(rm);
            return true;
        }
	}

    public boolean batch_insert_superColumn(batch_mutation_super_t batchMutationSuper, int block_for)
    {
        logger_.debug("batch_insert_SuperColumn");
        RowMutation rm = RowMutation.getRowMutation(batchMutationSuper);
        if (block_for > 0) {
            return insertBlocking(rm);
        } else {
            insert(rm);
            return true;
        }
    }


    // methods thrift wants
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
    public String getCpuProfile(int i) throws TException {
        return null;
    }

	public static void main(String[] args) throws Throwable
	{
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                logger_.error("Fatal exception in thread " + t, e);
            }
        });

		try
		{
			CassandraServer server = new CassandraServer();
			server.start();
            TThreadPoolServer threadPoolServer = thriftEngine(server);
            threadPoolServer.serve();
        }
		catch (Throwable x)
		{
            logger_.error("Fatal error; exiting", x);
			System.exit(1);
		}

	}

    private static TThreadPoolServer thriftEngine(CassandraServer server) throws TTransportException {
        Cassandra.Processor processor = new Cassandra.Processor(server);
        // Transport
        TServerSocket tServerSocket =  new TServerSocket(9160);
        // Protocol factory
        TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory();
        // ThreadPool Server
        TThreadPoolServer.Options options = new TThreadPoolServer.Options();
        options.minWorkerThreads = 64;
        return new TThreadPoolServer(new TProcessorFactory(processor),
                                     tServerSocket,
                                     new TTransportFactory(),
                                     new TTransportFactory(),
                                     tProtocolFactory,
                                     tProtocolFactory,
                                     options);
    }
}
