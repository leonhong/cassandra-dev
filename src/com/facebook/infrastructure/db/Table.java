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

import com.facebook.infrastructure.analytics.DBAnalyticsSource;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.dht.BootstrapInitiateMessage;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.SSTable;
import com.facebook.infrastructure.net.*;
import com.facebook.infrastructure.net.io.IStreamComplete;
import com.facebook.infrastructure.net.io.StreamContextManager;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.BasicUtilities;
import com.facebook.infrastructure.utils.CountingBloomFilter;
import com.facebook.infrastructure.utils.FBUtilities;
import com.facebook.infrastructure.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ExecutionException;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
*/

public class Table
{
    private static Logger logger_ = Logger.getLogger(Table.class);
    public static final String newLine_ = System.getProperty("line.separator");
    public static final String recycleBin_ = "RecycleColumnFamily";
    public static final String hints_ = "HintsColumnFamily";

    /* Used to lock the factory for creation of Table instance */
    private static Lock createLock_ = new ReentrantLock();
    private static Map<String, Table> instances_ = new HashMap<String, Table>();
    /* Table name. */
    private String table_;
    /* Handle to the Table Metadata */
    private TableMetadata tableMetadata_;
    /* ColumnFamilyStore per column family */
    private Map<String, ColumnFamilyStore> columnFamilyStores_ = new HashMap<String, ColumnFamilyStore>();
    /* The AnalyticsSource instance which keeps track of statistics reported to Ganglia. */
    private DBAnalyticsSource dbAnalyticsSource_;

    /*
     * Create the metadata tables. This table has information about
     * the table name and the column families that make up the table.
     * Each column family also has an associated ID which is an int.
    */
    static {
        Map<String, Set<String>> columnFamilyMap = DatabaseDescriptor.getTableToColumnFamilyMap();
        AtomicInteger idGenerator = new AtomicInteger(0);
        Set<String> tables = columnFamilyMap.keySet();

        TableMetadata tmetadata = null;
        try {
            tmetadata = TableMetadata.instance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for ( String table : tables )
        {
            if ( tmetadata.isEmpty() )
            {
                /* Column families associated with this table */
                Set<String> columnFamilies = columnFamilyMap.get(table);
                for ( String columnFamily : columnFamilies )
                {
                    tmetadata.add(columnFamily, idGenerator.getAndIncrement(), DatabaseDescriptor.getColumnType(columnFamily));
                }

                /*
                 * Here we add all the system related column families.
                */
                /* Add the TableMetadata column family to this map. */
                tmetadata.add(TableMetadata.cfName_, idGenerator.getAndIncrement());
                /* Add the LocationInfo column family to this map. */
                tmetadata.add(SystemTable.cfName_, idGenerator.getAndIncrement());
                /* Add the recycle column family to this map. */
                tmetadata.add(recycleBin_, idGenerator.getAndIncrement());
                /* Add the Hints column family to this map. */
                tmetadata.add(hints_, idGenerator.getAndIncrement(), ColumnFamily.getColumnType("Super"));
                try {
                    tmetadata.apply();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                idGenerator.set(0);
            }
        }
    }

    /**
     * This is the callback handler that is invoked when we have
     * completely been bootstrapped for a single file by a remote host.
    */
    public static class BootstrapCompletionHandler implements IStreamComplete
    {                
        public void onStreamCompletion(String host, StreamContextManager.StreamContext streamContext, StreamContextManager.StreamStatus streamStatus) throws IOException
        {                        
            /* Parse the stream context and the file to the list of SSTables in the associated Column Family Store. */            
            if ( streamContext.getTargetFile().indexOf("-Data.db") != -1 )
            {
                File file = new File( streamContext.getTargetFile() );
                String fileName = file.getName();
                /*
                 * If the file is a Data File we need to load the indicies associated
                 * with this file. We also need to cache the file name in the SSTables
                 * list of the associated Column Family. Also merge the CBF into the
                 * sampler.
                */
                SSTable.maybeLoadIndexFile(streamContext.getTargetFile());
                logger_.debug("Merging the counting bloom filter in the sampler ...");
                String[] peices = FBUtilities.strip(fileName, "-");
                Table.open(peices[0]).getColumnFamilyStore(peices[1]).addToList(streamContext.getTargetFile());                
            }
            
            EndPoint to = new EndPoint(host, DatabaseDescriptor.getStoragePort());
            logger_.debug("Sending a bootstrap terminate message with " + streamStatus + " to " + to);
            /* Send a StreamStatusMessage object which may require the source node to re-stream certain files. */
            StreamContextManager.StreamStatusMessage streamStatusMessage = new StreamContextManager.StreamStatusMessage(streamStatus);
            Message message = StreamContextManager.StreamStatusMessage.makeStreamStatusMessage(streamStatusMessage);
            MessagingService.getMessagingInstance().sendOneWay(message, to);           
        }
    }

    public static class BootStrapInitiateVerbHandler implements IVerbHandler<byte[]>
    {
        /*
         * Here we handle the BootstrapInitiateMessage. Here we get the
         * array of StreamContexts. We get file names for the column
         * families associated with the files and replace them with the
         * file names as obtained from the column family store on the
         * receiving end.
        */

        public void doVerb(Message<byte[]> message) {
            byte[] body = message.getMessageBody();
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length); 
            
            try
            {
                BootstrapInitiateMessage biMsg = BootstrapInitiateMessage.serializer().deserialize(bufIn);
                StreamContextManager.StreamContext[] streamContexts = biMsg.getStreamContext();                
                
                Map<String, String> fileNames = getNewNames(streamContexts);
                /*
                 * For each of stream context's in the incoming message
                 * generate the new file names and store the new file names
                 * in the StreamContextManager.
                */
                for (StreamContextManager.StreamContext streamContext : streamContexts )
                {                    
                    StreamContextManager.StreamStatus streamStatus = new StreamContextManager.StreamStatus(streamContext.getTargetFile(), streamContext.getExpectedBytes() );
                    File sourceFile = new File( streamContext.getTargetFile() );
                    String[] peices = FBUtilities.strip(sourceFile.getName(), "-");
                    String newFileName = fileNames.get( peices[1] + "-" + peices[2] );
                    
                    String file = new String(DatabaseDescriptor.getDataFileLocation() + System.getProperty("file.separator") + newFileName + "-Data.db");
                    logger_.debug("Received Data from  : " + message.getFrom() + " " + streamContext.getTargetFile() + " " + file);
                    streamContext.setTargetFile(file);
                    addStreamContext(message.getFrom().getHost(), streamContext, streamStatus);                                            
                }    
                                             
                StreamContextManager.registerStreamCompletionHandler(message.getFrom().getHost(), new Table.BootstrapCompletionHandler());
                /* Send a bootstrap initiation done message to execute on default stage. */
                logger_.debug("Sending a bootstrap initiate done message ...");                
                Message doneMessage = new Message( StorageService.getLocalStorageEndPoint(), "", StorageService.bootStrapInitiateDoneVerbHandler_, ArrayUtils.EMPTY_BYTE_ARRAY );
                MessagingService.getMessagingInstance().sendOneWay(doneMessage, message.getFrom());
            }
            catch ( IOException ex )
            {
                logger_.info(LogUtil.throwableToString(ex));
            }
        }
        
        private Map<String, String> getNewNames(StreamContextManager.StreamContext[] streamContexts)
        {
            /*
             * Mapping for each file with unique CF-i ---> new file name. For eg.
             * for a file with name <Table>-<CF>-<i>-Data.db there is a corresponding
             * <Table>-<CF>-<i>-Index.db. We maintain a mapping from <CF>-<i> to a newly
             * generated file name.
            */
            Map<String, String> fileNames = new HashMap<String, String>();
            /* Get the distinct entries from StreamContexts i.e have one entry per Data/Index file combination */
            Set<String> distinctEntries = new HashSet<String>();
            for ( StreamContextManager.StreamContext streamContext : streamContexts )
            {
                String[] peices = FBUtilities.strip(streamContext.getTargetFile(), "-");
                distinctEntries.add(peices[1] + "-" + peices[2]);
            }

            /* Generate unique file names per entry */
            Table table = Table.open( DatabaseDescriptor.getTables().get(0) );

            for ( String distinctEntry : distinctEntries )
            {
                String[] peices = FBUtilities.strip(distinctEntry, "-");
                ColumnFamilyStore cfStore = table.columnFamilyStores_.get(peices[0]);
                logger_.debug("Generating file name for " + distinctEntry + " ...");
                fileNames.put(distinctEntry, cfStore.getNextFileName());
            }

            return fileNames;
        }

        private boolean isStreamContextForThisColumnFamily(StreamContextManager.StreamContext streamContext, String cf)
        {
            String[] peices = FBUtilities.strip(streamContext.getTargetFile(), "-");
            return peices[1].equals(cf);
        }
        
        private String getColumnFamilyFromFile(String file)
        {
            String[] peices = FBUtilities.strip(file, "-");
            return peices[1];
        }
                
        private void addStreamContext(String host, StreamContextManager.StreamContext streamContext, StreamContextManager.StreamStatus streamStatus)
        {
            logger_.debug("Adding stream context " + streamContext + " for " + host + " ...");
            StreamContextManager.addStreamContext(host, streamContext, streamStatus);
        }
    }
    
    public static Table open(String table)
    {
        Table tableInstance = instances_.get(table);
        /*
         * Read the config and figure the column families for this table.
         * Set the isConfigured flag so that we do not read config all the
         * time.
        */
        if ( tableInstance == null )
        {
            Table.createLock_.lock();
            try
            {
                if ( tableInstance == null )
                {
                    tableInstance = new Table(table);
                    instances_.put(table, tableInstance);
                }
            }
            finally
            {
                createLock_.unlock();
            }
        }
        return tableInstance;
    }


    public Set<String> getColumnFamilies()
    {
        return tableMetadata_.getColumnFamilies();
    }

    public Set<String> getApplicationColumnFamilies() {
        Set<String> set = new HashSet<String>();
        for (String cfName : getColumnFamilies()) {
            if (DatabaseDescriptor.isApplicationColumnFamily(cfName)) {
                set.add(cfName);
            }
        }
        return set;
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        return columnFamilyStores_.get(cfName);
    }
    
    String getColumnFamilyType(String cfName)
    {
        String cfType = null;
        if ( tableMetadata_ != null )
          cfType = tableMetadata_.getColumnFamilyType(cfName);
        return cfType;
    }

    public void setColumnFamilyType(String cfName, String type)
    {
        tableMetadata_.setColumnFamilyType(cfName, type);
    }
    
    /*
     * This method is called to obtain statistics about
     * the table. It will return statistics about all
     * the column families that make up this table. 
    */
    public String tableStats(String newLineSeparator, java.text.DecimalFormat df)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(table_ + " statistics :");
        sb.append(newLineSeparator);
        int oldLength = sb.toString().length();
        
        Set<String> cfNames = columnFamilyStores_.keySet();
        for ( String cfName : cfNames )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(cfName);
            sb.append(cfStore.cfStats(newLineSeparator, df));
        }
        int newLength = sb.toString().length();
        
        /* Don't show anything if there is nothing to show. */
        if ( newLength == oldLength )
            return "";
        
        return sb.toString();
    }

    void onStart() throws IOException
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                cfStore.onStart();
        }        
    }
    
    /** 
     * Do a cleanup of keys that do not belong locally.
     */
    public void doGC()
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                cfStore.forceCleanup();
        }   
    }
    
    
    /*
     * This method is used to ensure that all keys
     * prior to the specified key, as dtermined by
     * the SSTable index bucket it falls in, are in
     * buffer cache.  
    */
    public void touch(String key, boolean fData) throws IOException
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            if ( DatabaseDescriptor.isApplicationColumnFamily(columnFamily) )
            {
                ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
                if ( cfStore != null )
                    cfStore.touch(key, fData);
            }
        }
    }
    
    /*
     * This method is invoked only during a bootstrap process. We basically
     * do a complete compaction since we can figure out based on the ranges
     * whether the files need to be split.
    */
    public CountingBloomFilter forceCompaction(List<Range> ranges, EndPoint target, List<String> fileList) throws IOException, ExecutionException, InterruptedException {
        /* Counting Bloom Filter for the entire table */
        CountingBloomFilter cbf = null;
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            if ( !isApplicationColumnFamily(columnFamily) )
                continue;
            
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
            {
                /* Counting Bloom Filter for the Column Family */
                CountingBloomFilter cbf2 = cfStore.forceCompaction(ranges, target, 0, fileList);
                assert cbf2 != null;
                if (cbf == null) {
                    cbf = cbf2;
                } else {
                    cbf.merge(cbf2);
                }
            }
        }
        return cbf;
    }
    
    /*
     * This method is an ADMIN operation to force compaction
     * of all SSTables on disk. 
    */
    public void forceCompaction() throws IOException
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                CompactionManager.instance().submitMajor(cfStore, null, 0);
        }
    }

    /*
     * Get the list of all SSTables on disk. 
    */
    public List<String> getAllSSTablesOnDisk()
    {
        List<String> list = new ArrayList<String>();
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                list.addAll( cfStore.getAllSSTablesOnDisk() );
        }
        return list;
    }

    private Table(String table)
    {
        table_ = table;
        dbAnalyticsSource_ = new DBAnalyticsSource();
        try
        {
            tableMetadata_ = TableMetadata.instance();
            Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
            for ( String columnFamily : columnFamilies )
            {
                columnFamilyStores_.put( columnFamily, new ColumnFamilyStore(table, columnFamily) );
            }
        }
        catch ( IOException ex )
        {
            logger_.info(LogUtil.throwableToString(ex));
        }
    }

    String getTableName()
    {
        return table_;
    }
    
    boolean isApplicationColumnFamily(String columnFamily)
    {
        return DatabaseDescriptor.isApplicationColumnFamily(columnFamily);
    }

    int getNumberOfColumnFamilies()
    {
        return tableMetadata_.size();
    }

    int getColumnFamilyId(String columnFamily)
    {
        return tableMetadata_.getColumnFamilyId(columnFamily);
    }

    String getColumnFamilyName(int id)
    {
        return tableMetadata_.getColumnFamilyName(id);
    }
    
    public CountingBloomFilter cardinality()
    {
        return tableMetadata_.cardinality();
    }

    boolean isValidColumnFamily(String columnFamily)
    {
        return tableMetadata_.isValidColumnFamily(columnFamily);
    }

    /*
     * Selects the row associated with the given key.
    */
    Row get(String key) throws IOException
    {        
        Row row = new Row(key);
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        long start = System.currentTimeMillis();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily);
            if ( cfStore != null )
            {                
                ColumnFamily cf = cfStore.getColumnFamily(key, columnFamily, new IdentityFilter());                
                if ( cf != null )
                    row.addColumnFamily(cf);
            }
        }
        
        long timeTaken = System.currentTimeMillis() - start;
        dbAnalyticsSource_.updateReadStatistics(timeTaken);
        return row;
    }

    /*
     * Selects the specified column family for the specified key.
    */
    public ColumnFamily get(String key, String cf) throws ColumnFamilyNotDefinedException, IOException
    {
        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        long start = System.currentTimeMillis();
        ColumnFamilyStore cfStore = columnFamilyStores_.get(values[0]);
        if ( cfStore != null )
        {
            ColumnFamily columnFamily = cfStore.getColumnFamily(key, cf, new IdentityFilter());
            long timeTaken = System.currentTimeMillis() - start;
            dbAnalyticsSource_.updateReadStatistics(timeTaken);
            return columnFamily;
        }
        else
        {
            throw new ColumnFamilyNotDefinedException("Column family " + cf + " has not been defined");
        }
    }

    /*
     * Selects only the specified column family for the specified key.
    */
    public Row getRow(String key, String cf) throws ColumnFamilyNotDefinedException, IOException
    {
        Row row = new Row(key);
        ColumnFamily columnFamily = get(key, cf);
        if ( columnFamily != null )
        	row.addColumnFamily(columnFamily);
        return row;
    }

    /* 
     * Selects only the specified column family for the specified key.
    */
    public Row getRow(String key, String cf, int start, int count) throws ColumnFamilyNotDefinedException, IOException
    {
        Row row = new Row(key);
        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        ColumnFamilyStore cfStore = columnFamilyStores_.get(values[0]);
        long start1 = System.currentTimeMillis();
        if ( cfStore != null )
        {
            ColumnFamily columnFamily = cfStore.getColumnFamily(key, cf, new CountFilter(count));
            if ( columnFamily != null )
                row.addColumnFamily(columnFamily);
            long timeTaken = System.currentTimeMillis() - start1;
            dbAnalyticsSource_.updateReadStatistics(timeTaken);
            return row;
        }
        else
            throw new ColumnFamilyNotDefinedException("Column family " + cf + " has not been defined");
    }
    
    public Row getRow(String key, String cf, long sinceTimeStamp) throws ColumnFamilyNotDefinedException, IOException
    {
        Row row = new Row(key);
        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        ColumnFamilyStore cfStore = columnFamilyStores_.get(values[0]);
        long start1 = System.currentTimeMillis();
        if ( cfStore != null )
        {
            ColumnFamily columnFamily = cfStore.getColumnFamily(key, cf, new TimeFilter(sinceTimeStamp));
            if ( columnFamily != null )
                row.addColumnFamily(columnFamily);
            long timeTaken = System.currentTimeMillis() - start1;
            dbAnalyticsSource_.updateReadStatistics(timeTaken);
            return row;
        }
        else
            throw new ColumnFamilyNotDefinedException("Column family " + cf + " has not been defined");
    }

    /*
     * This method returns the specified columns for the specified
     * column family.
     * 
     *  param @ key - key for which data is requested.
     *  param @ cf - column family we are interested in.
     *  param @ columns - columns that are part of the above column family.
    */
    public Row getRow(String key, String cf, List<String> columns) throws IOException
    {
    	Row row = new Row(key);
        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        ColumnFamilyStore cfStore = columnFamilyStores_.get(values[0]);

        if ( cfStore != null )
        {
        	ColumnFamily columnFamily = cfStore.getColumnFamily(key, cf, new NamesFilter(new ArrayList(columns)));
        	if ( columnFamily != null )
        		row.addColumnFamily(columnFamily);
        }
    	return row;
    }


    /*
     * This method adds the row to the Commit Log associated with this table.
     * Once this happens the data associated with the individual column families
     * is also written to the column family store's memtable.
    */
    void apply(Row row) throws IOException
    {
        /* Add row to the commit log. */
        long start = System.currentTimeMillis();
        CommitLog.CommitLogContext cLogCtx = CommitLog.open(table_).add(row);

        for ( ColumnFamily columnFamily : row.getColumnFamilies() )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily.name());
            cfStore.apply(row.key(), columnFamily, cLogCtx);
        }

        long timeTaken = System.currentTimeMillis() - start;
        dbAnalyticsSource_.updateWriteStatistics(timeTaken);
    }

    void applyNow(Row row) throws IOException
    {
        String key = row.key();
        Map<String, ColumnFamily> columnFamilies = row.getColumnFamilyMap();

        Set<String> cNames = columnFamilies.keySet();
        for ( String cName : cNames )
        {
            ColumnFamily columnFamily = columnFamilies.get(cName);
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily.name());
            cfStore.applyNow( key, columnFamily );
        }
    }

    public void flush(boolean fRecovery) throws IOException
    {
        Set<String> cfNames = columnFamilyStores_.keySet();
        for ( String cfName : cfNames )
        {
            if (fRecovery) {
                columnFamilyStores_.get(cfName).flushMemtable();
            } else {
                columnFamilyStores_.get(cfName).forceFlush();
            }
        }
    }

    void load(Row row) throws IOException, ExecutionException, InterruptedException {
        String key = row.key();
        /* Add row to the commit log. */
        long start = System.currentTimeMillis();
                
        Map<String, ColumnFamily> columnFamilies = row.getColumnFamilyMap();
        Set<String> cNames = columnFamilies.keySet();
        for ( String cName : cNames )
        {
        	if( cName.equals(Table.recycleBin_))
        	{
	        	ColumnFamily columnFamily = columnFamilies.get(cName);
	        	Collection<IColumn> columns = columnFamily.getAllColumns();
        		for(IColumn column : columns)
        		{
    	            ColumnFamilyStore cfStore = columnFamilyStores_.get(column.name());
    	            if(column.timestamp() == 1)
    	            {
    	            	cfStore.forceFlushBinary();
    	            }
    	            else if(column.timestamp() == 2)
    	            {
    	            	cfStore.forceCompaction(null, null, BasicUtilities.byteArrayToLong(column.value()), null);
    	            }
    	            else if(column.timestamp() == 3)
    	            {
    	            	cfStore.forceFlush();
    	            }
    	            else if(column.timestamp() == 4)
    	            {
    	            	cfStore.forceCleanup();
    	            }
    	            else
    	            {
    	            	cfStore.applyBinary(key, column.value());
    	            }
        		}
        	}
        }
        row.clear();
        long timeTaken = System.currentTimeMillis() - start;
        dbAnalyticsSource_.updateWriteStatistics(timeTaken);
    }
}
