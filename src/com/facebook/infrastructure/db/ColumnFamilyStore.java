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

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.log4j.Logger;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.io.*;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.*;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ColumnFamilyStore
{
    static class FileStructComparator implements Comparator<ColumnFamilyCompactor.FileStruct>
    {
        public int compare(ColumnFamilyCompactor.FileStruct f, ColumnFamilyCompactor.FileStruct f2)
        {
            return f.reader.getFileName().compareTo(f2.reader.getFileName());
        }

        public boolean equals(Object o)
        {
            if (!(o instanceof FileStructComparator))
                return false;
            return true;
        }
    }

    static class FileNameComparator implements Comparator<String>
    {
        public int compare(String f, String f2)
        {
            return getIndexFromFileName(f2) - getIndexFromFileName(f);
        }

        public boolean equals(Object o)
        {
            if (!(o instanceof FileNameComparator))
                return false;
            return true;
        }
    }

    private static int threshHold_ = 4;
    private static final int bufSize_ = 128*1024*1024;
    private static Logger logger_ = Logger.getLogger(ColumnFamilyStore.class);

    private String table_;
    public String columnFamily_;

    /* This is used to generate the next index for a SSTable */
    private AtomicInteger fileIndexGenerator_ = new AtomicInteger(0);

    /* memtable associated with this ColumnFamilyStore. */
    private AtomicReference<Memtable> memtable_;
    private AtomicReference<BinaryMemtable> binaryMemtable_;

    /* SSTables on disk for this column family */
    private Set<String> ssTables_ = new HashSet<String>();

    /* Modification lock used for protecting reads from compactions. */
    private ReentrantReadWriteLock lock_ = new ReentrantReadWriteLock(true);

    /* Flag indicates if a compaction is in process */
    public AtomicBoolean isCompacting_ = new AtomicBoolean(false);


    ColumnFamilyStore(String table, String columnFamily) throws IOException
    {
        table_ = table;
        columnFamily_ = columnFamily;
        /*
         * Get all data files associated with old Memtables for this table.
         * These files are named as follows <Table>-1.db, ..., <Table>-n.db. Get
         * the max which in this case is n and increment it to use it for next
         * index.
         */
        List<Integer> indices = new ArrayList<Integer>();
        String[] dataFileDirectories = DatabaseDescriptor.getAllDataFileLocations();
        for ( String directory : dataFileDirectories )
        {
            File fileDir = new File(directory);
            File[] files = fileDir.listFiles();
            for (File file : files)
            {
                String filename = file.getName();
                String[] tblCfName = getTableAndColumnFamilyName(filename);
                if (tblCfName[0].equals(table_)
                        && tblCfName[1].equals(columnFamily))
                {
                    int index = getIndexFromFileName(filename);
                    indices.add(index);
                }
            }
        }
        Collections.sort(indices);
        int value = (indices.size() > 0) ? (indices.get(indices.size() - 1)) : 0;
        fileIndexGenerator_.set(value);
        memtable_ = new AtomicReference<Memtable>( new Memtable(table_, columnFamily_) );
        binaryMemtable_ = new AtomicReference<BinaryMemtable>( new BinaryMemtable(table_, columnFamily_) );
    }

    void onStart() throws IOException
    {
        /* Do major compaction */
        List<File> ssTables = new ArrayList<File>();
        String[] dataFileDirectories = DatabaseDescriptor.getAllDataFileLocations();
        for ( String directory : dataFileDirectories )
        {
            File fileDir = new File(directory);
            File[] files = fileDir.listFiles();
            for (File file : files)
            {
                String filename = file.getName();
                if(((file.length() == 0) || (filename.indexOf("-" + SSTable.temporaryFile_) != -1) ) && (filename.indexOf(columnFamily_) != -1))
                {
                	file.delete();
                	continue;
                }
                String[] tblCfName = getTableAndColumnFamilyName(filename);
                if (tblCfName[0].equals(table_)
                        && tblCfName[1].equals(columnFamily_)
                        && filename.indexOf("-Data.db") != -1)
                {
                    ssTables.add(file.getAbsoluteFile());
                }
            }
        }
        Collections.sort(ssTables, new FileUtils.FileComparator());
        List<String> filenames = new ArrayList<String>();
        for (File ssTable : ssTables)
        {
            filenames.add(ssTable.getAbsolutePath());
        }

        /* There are no files to compact just add to the list of SSTables */
        ssTables_.addAll(filenames);
        /* Load the index files and the Bloom Filters associated with them. */
        SSTable.onStart(filenames);
        logger_.debug("Submitting a major compaction task ...");
        MinorCompactionManager.instance().submit(ColumnFamilyStore.this);
        if(columnFamily_.equals(Table.hints_))
        {
        	HintedHandOffManager.instance().submit(this);
        }
        MinorCompactionManager.instance().submitPeriodicCompaction(this);
    }

    List<String> getAllSSTablesOnDisk()
    {
        return new ArrayList<String>(ssTables_);
    }

    /*
     * This method is called to obtain statistics about
     * the Column Family represented by this Column Family
     * Store. It will report the total number of files on
     * disk and the total space oocupied by the data files
     * associated with this Column Family.
    */
    public String cfStats(String newLineSeparator, java.text.DecimalFormat df)
    {
        StringBuilder sb = new StringBuilder();
        /*
         * We want to do this so that if there are
         * no files on disk we do not want to display
         * something ugly on the admin page.
        */
        if ( ssTables_.size() == 0 )
        {
            return sb.toString();
        }
        sb.append(columnFamily_ + " statistics :");
        sb.append(newLineSeparator);
        sb.append("Number of files on disk : " + ssTables_.size());
        sb.append(newLineSeparator);
        double totalSpace = 0d;
        for ( String file : ssTables_ )
        {
            File f = new File(file);
            totalSpace += f.length();
        }
        String diskSpace = FileUtils.stringifyFileSize(totalSpace);
        sb.append("Total disk space : " + diskSpace);
        sb.append(newLineSeparator);
        sb.append("--------------------------------------");
        sb.append(newLineSeparator);
        return sb.toString();
    }

    /*
     * This is called after bootstrap to add the files
     * to the list of files maintained.
    */
    void addToList(String file)
    {
    	lock_.writeLock().lock();
        try
        {
            ssTables_.add(file);
        }
        finally
        {
        	lock_.writeLock().unlock();
        }
    }

    void touch(String key, boolean fData) throws IOException
    {
        /* Scan the SSTables on disk first */
        lock_.readLock().lock();
        try
        {
            List<String> files = new ArrayList<String>(ssTables_);
            for (String file : files)
            {
                /*
                 * Get the BloomFilter associated with this file. Check if the key
                 * is present in the BloomFilter. If not continue to the next file.
                */
                boolean bVal = SSTable.isKeyInFile(key, file);
                if ( !bVal )
                    continue;
                SSTable ssTable = new SSTable(file);
                ssTable.touch(key, fData);
            }
        }
        finally
        {
            lock_.readLock().unlock();
        }
    }

    /*
     * This method forces a compaction of the SSTables on disk. We wait
     * for the process to complete by waiting on a future pointer.
    */
    BloomFilter.CountingBloomFilter forceCompaction(List<Range> ranges, EndPoint target, long skip, List<String> fileList)
    {
        BloomFilter.CountingBloomFilter cbf = null;
    	Future<BloomFilter.CountingBloomFilter> futurePtr = null;
    	if( ranges != null)
    		futurePtr = MinorCompactionManager.instance().submit(ColumnFamilyStore.this, ranges, target, fileList);
    	else
    		futurePtr = MinorCompactionManager.instance().submitMajor(ColumnFamilyStore.this, ranges, skip);

        try
        {
            /* Waiting for the compaction to complete. */
            cbf = futurePtr.get();
            logger_.debug("Done forcing compaction ...");
        }
        catch (ExecutionException ex)
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
        catch ( InterruptedException ex2 )
        {
            logger_.debug(LogUtil.throwableToString(ex2));
        }
        return cbf;
    }

    String getColumnFamilyName()
    {
        return columnFamily_;
    }

    private String[] getTableAndColumnFamilyName(String filename)
    {
        StringTokenizer st = new StringTokenizer(filename, "-");
        String[] values = new String[2];
        int i = 0;
        while (st.hasMoreElements())
        {
            if (i == 0)
                values[i] = (String) st.nextElement();
            else if (i == 1)
            {
                values[i] = (String) st.nextElement();
                break;
            }
            ++i;
        }
        return values;
    }

    protected static int getIndexFromFileName(String filename)
    {
        /*
         * File name is of the form <table>-<column family>-<index>-Data.db.
         * This tokenizer will strip the .db portion.
         */
        StringTokenizer st = new StringTokenizer(filename, "-");
        /*
         * Now I want to get the index portion of the filename. We accumulate
         * the indices and then sort them to get the max index.
         */
        int count = st.countTokens();
        int i = 0;
        String index = null;
        while (st.hasMoreElements())
        {
            index = (String) st.nextElement();
            if (i == (count - 2))
                break;
            ++i;
        }
        return Integer.parseInt(index);
    }

    String getNextFileName()
    {
        String name = table_ + "-" + columnFamily_ + "-" + fileIndexGenerator_.incrementAndGet();
        return name;
    }

    /*
     * Return a temporary file name.
     */
    public String getTempFileName()
    {
        String name = table_ + "-" + columnFamily_ + "-" + SSTable.temporaryFile_ + "-" + fileIndexGenerator_.incrementAndGet() ;
        return name;
    }

    /*
     * This version is used only on start up when we are recovering from logs.
     * In the future we may want to parellelize the log processing for a table
     * by having a thread per log file present for recovery. Re-visit at that
     * time.
     */
    void switchMemtable(String key, ColumnFamily columnFamily, CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        memtable_.set( new Memtable(table_, columnFamily_) );
        if(!key.equals(Memtable.flushKey_))
        	memtable_.get().put(key, columnFamily, cLogCtx);
    }

    /*
     * This version is used when we forceflush.
     */
    void switchMemtable() throws IOException
    {
        memtable_.set( new Memtable(table_, columnFamily_) );
    }

    /*
     * This version is used only on start up when we are recovering from logs.
     * In the future we may want to parellelize the log processing for a table
     * by having a thread per log file present for recovery. Re-visit at that
     * time.
     */
    void switchBinaryMemtable(String key, byte[] buffer) throws IOException
    {
        binaryMemtable_.set( new BinaryMemtable(table_, columnFamily_) );
        binaryMemtable_.get().put(key, buffer);
    }

    void forceFlush(boolean fRecovery) throws IOException
    {
        //MemtableManager.instance().submit(getColumnFamilyName(), memtable_.get() , CommitLog.CommitLogContext.NULL);
        //memtable_.get().flush(true, CommitLog.CommitLogContext.NULL);
        memtable_.get().forceflush(this, fRecovery);
    }

    void forceFlushBinary() throws IOException
    {
        BinaryMemtableManager.instance().submit(getColumnFamilyName(), binaryMemtable_.get());
        //binaryMemtable_.get().flush(true);
    }

    /*
     * Insert/Update the column family for this key. param @ lock - lock that
     * needs to be used. param @ key - key for update/insert param @
     * columnFamily - columnFamily changes
     */
    void apply(String key, ColumnFamily columnFamily, CommitLog.CommitLogContext cLogCtx)
            throws IOException
    {
        memtable_.get().put(key, columnFamily, cLogCtx);
    }

    /*
     * Insert/Update the column family for this key. param @ lock - lock that
     * needs to be used. param @ key - key for update/insert param @
     * columnFamily - columnFamily changes
     */
    void applyBinary(String key, byte[] buffer)
            throws IOException
    {
        binaryMemtable_.get().put(key, buffer);
    }

    /**
     *
     * Get the column family in the most efficient order.
     * 1. Memtable
     * 2. Sorted list of files
     */
    public ColumnFamily getColumnFamily(String key, String cf, IFilter filter) throws IOException
    {
    	List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
        long start = System.currentTimeMillis();
        /* Get the ColumnFamily from Memtable */
    	getColumnFamilyFromCurrentMemtable(key, cf, filter, columnFamilies);
        if (columnFamilies.size() == 0 || !filter.isDone()) {
            /* Check if MemtableManager has any historical information */
            MemtableManager.instance().getColumnFamily(key, columnFamily_, cf, filter, columnFamilies);
        }
        if (columnFamilies.size() == 0 || !filter.isDone()) {
            getColumnFamilyFromDisk(key, cf, columnFamilies, filter);
            logger_.debug("DISK TIME: " + (System.currentTimeMillis() - start) + " ms.");
        }
        return resolveAndRemoveDeleted(columnFamilies);
    }

    /**
     * Fetch from disk files and go in sorted order  to be efficient
     * This fn exits as soon as the required data is found.
     * @param key
     * @param cf
     * @param columnFamilies
     * @param filter
     * @throws IOException
     */
    private void getColumnFamilyFromDisk(String key, String cf, List<ColumnFamily> columnFamilies, IFilter filter) throws IOException
    {
        /* Scan the SSTables on disk first */
    	lock_.readLock().lock();
    	try
    	{
	        List<String> files = new ArrayList<String>(ssTables_);
	        Collections.sort(files, new FileNameComparator());
	        for (String file : files)
	        {
	            /*
	             * Get the BloomFilter associated with this file. Check if the key
	             * is present in the BloomFilter. If not continue to the next file.
	            */
                boolean bVal = SSTable.isKeyInFile(key, file);
                if ( !bVal )
                    continue;
	            ColumnFamily columnFamily = fetchColumnFamily(key, cf, filter, file);
	            long start = System.currentTimeMillis();
	            if (columnFamily != null)
	            {
		            /*
		             * TODO
		             * By using the filter before removing deleted columns (which is done by resolve())
		             * we have a efficient implementation of timefilter
		             * but for count filter this can return wrong results
		             * we need to take care of that later.
		             */
	                columnFamilies.add(columnFamily);
	                if(filter.isDone())
	                {
	                	break;
	                }
	            }
	            logger_.info("DISK Data structure population  TIME: " + (System.currentTimeMillis() - start)
	                    + " ms.");
	        }
	        files.clear();
    	}
    	finally
    	{
        	lock_.readLock().unlock();
    	}
    }


    private ColumnFamily fetchColumnFamily(String key, String cf, IFilter filter, String ssTableFile) throws IOException
	{
		SSTable ssTable = new SSTable(ssTableFile);
		long start = System.currentTimeMillis();
		DataInputBuffer bufIn = null;
		bufIn = filter.next(key, cf, ssTable);
		logger_.info("DISK ssTable.next TIME: " + (System.currentTimeMillis() - start) + " ms.");
		if (bufIn.getLength() == 0)
			return null;
        start = System.currentTimeMillis();
        ColumnFamily columnFamily = ColumnFamily.serializer().deserialize(bufIn, cf, filter);
		logger_.info("DISK Deserialize TIME: " + (System.currentTimeMillis() - start) + " ms.");
        return columnFamily;
	}



    private void getColumnFamilyFromCurrentMemtable(String key, String cf, IFilter filter, List<ColumnFamily> columnFamilies)
    {
        /* Get the ColumnFamily from Memtable */
        ColumnFamily columnFamily = memtable_.get().get(key, cf, filter);
        if (columnFamily != null)
        {
            columnFamilies.add(columnFamily);
        }
    }

    /** merge all columnFamilies into a single instance, with only the newest versions of columns preserved. */
    private static ColumnFamily resolve(List<ColumnFamily> columnFamilies)
    {
        int size = columnFamilies.size();
        if (size == 0)
            return null;

        // start from nothing so that we don't include potential deleted columns from the first instance
        String cfname = columnFamilies.get(0).name();
        ColumnFamily cf = new ColumnFamily(cfname);

        // merge
        for (ColumnFamily cf2 : columnFamilies)
        {
            assert cf.name().equals(cf2.name());
            cf.addColumns(cf2);
        }
        return cf;
    }

    /** like resolve, but leaves the resolved CF as the only item in the list */
    static void merge(List<ColumnFamily> columnFamilies)
    {
        ColumnFamily cf = resolve(columnFamilies);
        columnFamilies.clear();
        columnFamilies.add(cf);
    }


    static ColumnFamily resolveAndRemoveDeleted(List<ColumnFamily> columnFamilies) {
        ColumnFamily cf = resolve(columnFamilies);
        if (cf != null) {
            for (String cname : new ArrayList<String>(cf.getColumns().keySet())) {
                IColumn c = cf.getColumns().get(cname);
                if (c.isMarkedForDelete()) {
                    cf.remove(cname);
                } else if (c.getObjectCount() > 1) {
                    // don't operate directly on the supercolumn, it could be the one in the memtable
                    cf.remove(cname);
                    IColumn sc = cf.createColumn(c.name());
                    for (IColumn subColumn : c.getSubColumns()) {
                        if (!subColumn.isMarkedForDelete()) {
                            sc.addColumn(subColumn.name(), subColumn);
                        }
                    }
                    if (sc.getSubColumns().size() > 0) {
                        cf.addColumn(cname, sc);
                        logger_.debug("adding sc " + sc.name() + " to CF with " + sc.getSubColumns().size() + " columns: " + sc);
                    }
                }
            }
        }
        return cf;
    }


    /*
     * This version is used only on start up when we are recovering from logs.
     * Hence no locking is required since we process logs on the main thread. In
     * the future we may want to parellelize the log processing for a table by
     * having a thread per log file present for recovery. Re-visit at that time.
     */
    void applyNow(String key, ColumnFamily columnFamily) throws IOException
    {
        memtable_.get().putOnRecovery(key, columnFamily);
    }

    private void deleteSuperColumn(ColumnFamily columnFamily, SuperColumn c, long timestamp) {
        for (IColumn subColumn : c.getSubColumns()) {
            columnFamily.createColumn(c.name() + ":" + subColumn.name(), subColumn.value(), timestamp);
        }
        for (IColumn subColumn : columnFamily.getColumn(c.name()).getSubColumns()) {
            subColumn.delete();
        }
    }

    void delete(String key, ColumnFamily columnFamily)
            throws IOException
    {
        if (columnFamily.isMarkedForDelete()) {
            // mark individual columns deleted
            assert columnFamily.getAllColumns().size() == 0;
            ColumnFamily cf = getColumnFamily(key, columnFamily.name(), new IdentityFilter());
            for (IColumn c : cf.getAllColumns()) {
                if (cf.isSuper()) {
                    deleteSuperColumn(columnFamily, (SuperColumn)c, columnFamily.getMarkedForDeleteAt());
                } else {
                    columnFamily.createColumn(c.name(), c.value(), columnFamily.getMarkedForDeleteAt()).delete();
                }
            }
        } else if (columnFamily.isSuper()) {
            for (IColumn sc : columnFamily.getAllColumns()) {
                if (sc.isMarkedForDelete()) {
                    ColumnFamily cf = getColumnFamily(key, columnFamily.name(), new NamesFilter(Arrays.asList(new String[] { sc.name() })));
                    for (IColumn c : cf.getAllColumns()) {
                        deleteSuperColumn(columnFamily, (SuperColumn)c, sc.getMarkedForDeleteAt());
                    }
                }
            }
        }
        logger_.debug("deleting " + columnFamily);

        memtable_.get().remove(key, columnFamily);
    }

    /*
     * This method is called when the Memtable is frozen and ready to be flushed
     * to disk. This method informs the CommitLog that a particular ColumnFamily
     * is being flushed to disk.
     */
    void onMemtableFlush(CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        if ( cLogCtx.isValidContext() )
            CommitLog.open(table_).onMemtableFlush(columnFamily_, cLogCtx);
    }

    /*
     * Called after the Memtable flushes its in-memory data. This information is
     * cached in the ColumnFamilyStore. This is useful for reads because the
     * ColumnFamilyStore first looks in the in-memory store and the into the
     * disk to find the key. If invoked during recoveryMode the
     * onMemtableFlush() need not be invoked.
     *
     * param @ filename - filename just flushed to disk
     * param @ bf - bloom filter which indicates the keys that are in this file.
    */
    void storeLocation(String filename, BloomFilter bf) throws IOException
    {
        boolean doCompaction = false;
        int ssTableSize = 0;
    	lock_.writeLock().lock();
        try
        {
            ssTables_.add(filename);
            SSTable.storeBloomFilter(filename, bf);
            ssTableSize = ssTables_.size();
        }
        finally
        {
        	lock_.writeLock().unlock();
        }
        if (ssTableSize >= threshHold_ && !isCompacting_.get())
        {
            doCompaction = true;
        }

        if (isCompacting_.get())
        {
            if ( ssTableSize % threshHold_ == 0 )
            {
                doCompaction = true;
            }
        }
        if ( doCompaction )
        {
            logger_.debug("Submitting for  compaction ...");
            MinorCompactionManager.instance().submit(ColumnFamilyStore.this);
            logger_.debug("Submitted for compaction ...");
        }
    }

    /*
     * Stage the compactions , compact similar size files.
     * This fn figures out the files close enough by size and if they
     * are greater than the threshold then compacts.
     */
    Map<Integer, List<String>> stageCompaction(List<String> files)
    {
    	Map<Integer, List<String>>  buckets = new HashMap<Integer, List<String>>();
    	long averages[] = new long[100];
    	int count = 0 ;
    	long max = 200L*1024L*1024L*1024L;
    	long min = 50L*1024L*1024L;
    	List<String> largeFileList = new ArrayList<String>();
    	for(String file : files)
    	{
    		File f = new File(file);
    		long size = f.length();
    		if ( size > max)
    		{
    			largeFileList.add(file);
    			continue;
    		}
    		boolean bFound = false;
    		for ( int i = 0 ; i < count ; i++ )
    		{
    			if ( (size > averages[i]/2 && size < 3*averages[i]/2) || ( size < min && averages[i] < min ))
    			{
    				averages[i] = (averages[i] + size) / 2 ;
    				List<String> fileList = buckets.get(i);
    				if(fileList == null)
    				{
    					fileList = new ArrayList<String>();
    					buckets.put(i, fileList);
    				}
    				fileList.add(file);
    				bFound = true;
    				break;
    			}
    		}
    		if(!bFound)
    		{
				List<String> fileList = buckets.get(count);
				if(fileList == null)
				{
					fileList = new ArrayList<String>();
					buckets.put(count, fileList);
				}
				fileList.add(file);
    			averages[count] = size;
    			count++;
    		}

    	}
		// Put files greater than teh max in a separate bucket so that they are never compacted
		// but we need them in the buckets since for range compactions we need to split these files.
    	count++;
    	for(String file : largeFileList)
    	{
    		List<String> tempLargeFileList = new ArrayList<String>();
    		tempLargeFileList.add(file);
    		buckets.put(count, tempLargeFileList);
    		count++;
    	}
    	return buckets;
    }
    
    /*
     * Break the files into buckets and then compact.
     */
    BloomFilter.CountingBloomFilter doCompaction(List<Range> ranges)  throws IOException
    {
        isCompacting_.set(true);
        List<String> files = new ArrayList<String>(ssTables_);
        BloomFilter.CountingBloomFilter result = null;
        try
        {
	        int count = 0;
	    	Map<Integer, List<String>> buckets = stageCompaction(files);
	    	Set<Integer> keySet = buckets.keySet();
	    	for(Integer key : keySet)
	    	{
	    		List<String> fileList = buckets.get(key);
	            BloomFilter.CountingBloomFilter tempResult = null;
	    		// If ranges != null we should split the files irrespective of the threshold.
	    		if(fileList.size() >= threshHold_ || ranges != null)
	    		{
	    			files.clear();
	    			count = 0;
	    			for(String file : fileList)
	    			{
	    				files.add(file);
	    				count++;
	    				if( count == threshHold_ && ranges == null )
	    					break;
	    			}
	    	        try
	    	        {
	    	        	// For each bucket if it has crossed the threshhold do the compaction
	    	        	// In case of range  compaction merge the counting bloom filters also.
                        if(ranges == null )
                            tempResult = ColumnFamilyCompactor.doRangeCompaction(this, files, ranges, bufSize_);
                        else
                            tempResult = ColumnFamilyCompactor.doRangeCompaction(this, files, ranges, bufSize_);

	    	        	if(result == null)
	    	        	{
	    	        		result = tempResult;
	    	        	}
	    	        	else
	    	        	{
	    	        		result.merge(tempResult);
	    	        	}
	    	        }
	    	        catch ( Exception ex)
	    	        {
	    	        	ex.printStackTrace();
	    	        }
	    		}
	    	}
        }
        finally
        {
        	isCompacting_.set(false);
        }
        return result;
    }

    BloomFilter.CountingBloomFilter doMajorCompaction(long skip)  throws IOException
    {
    	return doMajorCompactionInternal( skip );
    }

    BloomFilter.CountingBloomFilter doMajorCompaction()  throws IOException
    {
    	return doMajorCompactionInternal( 0 );
    }
    /*
     * Compact all the files irrespective of the size.
     * skip : is the ammount in Gb of the files to be skipped
     * all files greater than skip GB are skipped for this compaction.
     * Except if skip is 0 , in that case this is ignored and all files are taken.
     */
    BloomFilter.CountingBloomFilter doMajorCompactionInternal(long skip)  throws IOException
    {
        isCompacting_.set(true);
        List<String> filesInternal = new ArrayList<String>(ssTables_);
        List<String> files = null;
        BloomFilter.CountingBloomFilter result = null;
        try
        {
        	 if( skip > 0L )
        	 {
        		 files = new ArrayList<String>();
	        	 for ( String file : filesInternal )
	        	 {
	        		 File f = new File(file);
	        		 if( f.length() < skip*1024L*1024L*1024L )
	        		 {
	        			 files.add(file);
	        		 }
	        	 }
        	 }
        	 else
        	 {
        		 files = filesInternal;
        	 }
        	 result = ColumnFamilyCompactor.doRangeCompaction(this, files, null, bufSize_);
        }
        catch ( Exception ex)
        {
        	ex.printStackTrace();
        }
        finally
        {
        	isCompacting_.set(false);
        }
        return result;
    }

    BloomFilter.CountingBloomFilter doRangeAntiCompaction(List<Range> ranges, EndPoint target, List<String> fileList) throws IOException
    {
        isCompacting_.set(true);
        List<String> files = new ArrayList<String>(ssTables_);
        BloomFilter.CountingBloomFilter result = null;
        try
        {
        	 result = ColumnFamilyCompactor.doRangeOnlyAntiCompaction(this, files, ranges, target, bufSize_, fileList, null);
        }
        catch ( Exception ex)
        {
        	ex.printStackTrace();
        }
        finally
        {
        	isCompacting_.set(false);
        }
        return result;

    }

    void forceCleanup()
    {
    	MinorCompactionManager.instance().submitCleanup(ColumnFamilyStore.this);
    }
    
    /**
     * This function goes over each file and removes the keys that the node is not responsible for 
     * and only keeps keys that this node is responsible for.
     * @throws IOException
     */
    void doCleanupCompaction() throws IOException
    {
        isCompacting_.set(true);
        List<String> files = new ArrayList<String>(ssTables_);
        for(String file: files)
        {
	        try
	        {
	        	doCleanup(file);
	        }
	        catch ( Exception ex)
	        {
	        	ex.printStackTrace();
	        }
        }
    	isCompacting_.set(false);
    }
    /**
     * cleans up one particular file by removing keys that this node is not responsible for.
     * @param file
     * @throws IOException
     */
    /* TODO: Take care of the comments later. */
    void doCleanup(String file) throws IOException
    {
    	if(file == null )
    		return;
        List<Range> myRanges = null;
    	List<String> files = new ArrayList<String>();
    	files.add(file);
    	List<String> newFiles = new ArrayList<String>();
    	Map<EndPoint, List<Range>> endPointtoRangeMap = StorageService.instance().constructEndPointToRangesMap();
    	myRanges = endPointtoRangeMap.get(StorageService.getLocalStorageEndPoint());
    	List<BloomFilter> compactedBloomFilters = new ArrayList<BloomFilter>();
        ColumnFamilyCompactor.doRangeOnlyAntiCompaction(this, files, myRanges, null, bufSize_, newFiles, compactedBloomFilters);
        logger_.info("Original file : " + file + " of size " + new File(file).length());
        lock_.writeLock().lock();
        try
        {
            ssTables_.remove(file);
            SSTable.removeAssociatedBloomFilter(file);
            for (String newfile : newFiles)
            {                            	
                logger_.info("New file : " + newfile + " of size " + new File(newfile).length());
                if ( newfile != null )
                {
                    ssTables_.add(newfile);
                    logger_.info("Inserting bloom filter for file " + newfile);
                    SSTable.storeBloomFilter(newfile, compactedBloomFilters.get(0));
                }
            }
            SSTable.delete(file);
        }
        finally
        {
            lock_.writeLock().unlock();
        }
    }


    long completeCompaction(List<String> files, String newfile, long totalBytesWritten, BloomFilter compactedBloomFilter) {
        lock_.writeLock().lock();
        try
	            {
	                for (String file : files)
            {
                ssTables_.remove(file);
                SSTable.removeAssociatedBloomFilter(file);
            }
            if ( newfile != null )
            {
                ssTables_.add(newfile);
                logger_.info("Inserting bloom filter for file " + newfile);
                SSTable.storeBloomFilter(newfile, compactedBloomFilter);
                totalBytesWritten = (new File(newfile)).length();
            }
        }
        finally
        {
            lock_.writeLock().unlock();
        }
        for (String file : files)
        {
            SSTable.delete(file);
        }
        return totalBytesWritten;
    }
    
}

