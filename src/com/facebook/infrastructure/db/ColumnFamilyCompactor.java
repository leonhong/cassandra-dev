package com.facebook.infrastructure.db;

import com.facebook.infrastructure.utils.BloomFilter;
import com.facebook.infrastructure.utils.LogUtil;
import com.facebook.infrastructure.utils.CountingBloomFilter;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.io.*;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.service.StorageService;

import java.util.*;
import java.io.IOException;
import java.io.File;
import java.math.BigInteger;

import org.apache.log4j.Logger;

public class ColumnFamilyCompactor {
    private static Logger logger_ = Logger.getLogger(ColumnFamilyCompactor.class);

    /**
     * This function is used to do the anti compaction process , it spits out the file which has keys that belong to a given range
     * If the target is not specified it spits out the file as a compacted file with the unecessary ranges wiped out.
     *
     * @param files
     * @param ranges
     * @param target
     * @param minBufferSize
     * @param fileList
     * @return
     * @throws java.io.IOException
     */
    static CountingBloomFilter doRangeOnlyAntiCompaction(
            ColumnFamilyStore cfs, List<String> files, List<Range> ranges, EndPoint target, int minBufferSize, List<String> fileList, List<BloomFilter> compactedBloomFilters) throws IOException {
        CountingBloomFilter rangeCountingBloomFilter = null;
        long startTime = System.currentTimeMillis();
        long totalBytesRead = 0;
        long totalBytesWritten = 0;
        long totalkeysRead = 0;
        long totalkeysWritten = 0;
        String rangeFileLocation = null;
        String mergedFileName = null;
        try {
            // Calculate the expected compacted filesize
            long expectedRangeFileSize = getExpectedCompactedFileSize(files);
            /* in the worst case a node will be giving out alf of its data so we take a chance */
            expectedRangeFileSize = expectedRangeFileSize / 2;
            rangeFileLocation = DatabaseDescriptor.getCompactionFileLocation(expectedRangeFileSize);
//	        boolean isLoop = isLoopAround( ranges );
//	        Range maxRange = getMaxRange( ranges );
            // If the compaction file path is null that means we have no space left for this compaction.
            if (rangeFileLocation == null) {
                logger_.warn("Total bytes to be written for range compaction  ..."
                        + expectedRangeFileSize + "   is greater than the safe limit of the disk space available.");
                return null;
            }
            PriorityQueue<FileStruct> pq = initializePriorityQueue(files, ranges, minBufferSize);
            if (pq.size() > 0) {
                mergedFileName = cfs.getTempFileName();
                SSTable ssTableRange = null;
                String lastkey = null;
                List<FileStruct> lfs = new ArrayList<FileStruct>();
                DataOutputBuffer bufOut = new DataOutputBuffer();
                int expectedBloomFilterSize = SSTable.getApproximateKeyCount(files);
                expectedBloomFilterSize = (expectedBloomFilterSize > 0) ? expectedBloomFilterSize : SSTable.indexInterval();
                logger_.debug("Expected bloom filter size : " + expectedBloomFilterSize);
                /* Create the bloom filter for the compacted file. */
                BloomFilter compactedRangeBloomFilter = new BloomFilter(expectedBloomFilterSize, 8);
                List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();

                while (pq.size() > 0 || lfs.size() > 0) {
                    FileStruct fs = null;
                    if (pq.size() > 0) {
                        fs = pq.poll();
                    }
                    if (fs != null
                            && (lastkey == null || lastkey.compareTo(fs.key) == 0)) {
                        // The keys are the same so we need to add this to the
                        // ldfs list
                        lastkey = fs.key;
                        lfs.add(fs);
                    } else {
                        Collections.sort(lfs, new ColumnFamilyStore.FileStructComparator());
                        ColumnFamily columnFamily = null;
                        bufOut.reset();
                        if (lfs.size() > 1) {
                            for (FileStruct filestruct : lfs) {
                                try {
                                    /* read the length although we don't need it */
                                    filestruct.bufIn.readInt();
                                    // Skip the Index
                                    if (DatabaseDescriptor.isNameIndexEnabled(cfs.getColumnFamilyName())) {
                                        IndexHelper.skip(filestruct.bufIn);
                                    }
                                    // We want to add only 2 and resolve them right there in order to save on memory footprint
                                    if (columnFamilies.size() > 1) {
                                        ColumnFamilyStore.merge(columnFamilies);
                                    }
                                    // deserialize into column families
                                    columnFamilies.add(ColumnFamily.serializer().deserialize(filestruct.bufIn));
                                }
                                catch (Exception ex) {
                                    logger_.warn(LogUtil.throwableToString(ex));
                                    continue;
                                }
                            }
                            // Now after merging all crap append to the sstable
                            columnFamily = ColumnFamilyStore.resolveAndRemoveDeleted(columnFamilies);
                            columnFamilies.clear();
                            if (columnFamily != null) {
                                /* serialize the cf with column indexes */
                                ColumnFamily.serializerWithIndexes().serialize(columnFamily, bufOut);
                            }
                        } else {
                            FileStruct filestruct = lfs.get(0);
                            try {
                                /* read the length although we don't need it */
                                int size = filestruct.bufIn.readInt();
                                bufOut.write(filestruct.bufIn, size);
                            }
                            catch (Exception ex) {
                                logger_.warn(LogUtil.throwableToString(ex));
                                filestruct.reader.close();
                                continue;
                            }
                        }
                        if (Range.isKeyInRanges(ranges, lastkey)) {
                            if (ssTableRange == null) {
                                if (target != null)
                                    rangeFileLocation = rangeFileLocation + System.getProperty("file.separator") + "bootstrap";
                                FileUtils.createDirectory(rangeFileLocation);
                                ssTableRange = new SSTable(rangeFileLocation, mergedFileName);
                            }
                            if (rangeCountingBloomFilter == null) {
                                rangeCountingBloomFilter = new CountingBloomFilter(expectedBloomFilterSize, 8);
                            }
                            try {
                                ssTableRange.append(lastkey, bufOut);
                                compactedRangeBloomFilter.add(lastkey);
                                if (target != null && StorageService.instance().isPrimary(lastkey, target)) {
                                    rangeCountingBloomFilter.add(lastkey);
                                }
                            }
                            catch (Exception ex) {
                                logger_.warn(LogUtil.throwableToString(ex));
                            }
                        }
                        totalkeysWritten++;
                        for (FileStruct filestruct : lfs) {
                            try {
                                filestruct.getNextKey();
                                if (filestruct.exhausted) {
                                    continue;
                                }
                                /* keep on looping until we find a key in the range */
                                while (!Range.isKeyInRanges(ranges, filestruct.key)) {
                                    fs.getNextKey();
                                    if (filestruct.exhausted) {
                                        break;
                                    }
                                    /* check if we need to continue , if we are done with ranges empty the queue and close all file handles and exit */
                                    //if( !isLoop && StorageService.hash(filestruct.key).compareTo(maxRange.right()) > 0 && !filestruct.key.equals(""))
                                    //{
                                    //filestruct.reader.close();
                                    //filestruct = null;
                                    //break;
                                    //}
                                }
                                if (filestruct != null) {
                                    pq.add(filestruct);
                                }
                                totalkeysRead++;
                            }
                            catch (Exception ex) {
                                // Ignore the exception as it might be a corrupted file
                                // in any case we have read as far as possible from it
                                // and it will be deleted after compaction.
                                logger_.warn(LogUtil.throwableToString(ex));
                                filestruct.reader.close();
                                continue;
                            }
                        }
                        lfs.clear();
                        lastkey = null;
                        if (fs != null) {
                            // Add back the fs since we processed the rest of
                            // filestructs
                            pq.add(fs);
                        }
                    }
                }
                if (ssTableRange != null) {
                    if (fileList == null)
                        fileList = new ArrayList<String>();
                    ssTableRange.closeRename(compactedRangeBloomFilter, fileList);
                    if (compactedBloomFilters != null)
                        compactedBloomFilters.add(compactedRangeBloomFilter);
                }
            }
        }
        catch (Exception ex) {
            logger_.warn(LogUtil.throwableToString(ex));
        }
        logger_.debug("Total time taken for range split   ..."
                + (System.currentTimeMillis() - startTime));
        logger_.debug("Total bytes Read for range split  ..." + totalBytesRead);
        logger_.debug("Total bytes written for range split  ..."
                + totalBytesWritten + "   Total keys read ..." + totalkeysRead);
        return rangeCountingBloomFilter;
    }/*
     * This function does the actual compaction for files.
     * It maintains a priority queue of with the first key from each file
     * and then removes the top of the queue and adds it to the SStable and
     * repeats this process while reading the next from each file until its
     * done with all the files . The SStable to which the keys are written
     * represents the new compacted file. Before writing if there are keys
     * that occur in multiple files and are the same then a resolution is done
     * to get the latest data.
     *
     */

    static CountingBloomFilter doRangeCompaction(ColumnFamilyStore cfs, List<String> files, List<Range> ranges, int minBufferSize) throws IOException {
        CountingBloomFilter rangeCountingBloomFilter = null;
        String newfile = null;
        long startTime = System.currentTimeMillis();
        long totalBytesRead = 0;
        long totalBytesWritten = 0;
        long totalkeysRead = 0;
        long totalkeysWritten = 0;
        try {
            // Calculate the expected compacted filesize
            long expectedCompactedFileSize = getExpectedCompactedFileSize(files);
            String compactionFileLocation = DatabaseDescriptor.getCompactionFileLocation(expectedCompactedFileSize);
            // If the compaction file path is null that means we have no space left for this compaction.
            if (compactionFileLocation == null) {
                if (ranges == null || ranges.size() == 0) {
                    String maxFile = getMaxSizeFile(files);
                    files.remove(maxFile);
                    return doRangeCompaction(cfs, files, ranges, minBufferSize);
                }
                logger_.warn("Total bytes to be written for compaction  ..."
                        + expectedCompactedFileSize + "   is greater than the safe limit of the disk space available.");
                return null;
            }
            PriorityQueue<FileStruct> pq = initializePriorityQueue(files, ranges, minBufferSize);
            if (pq.size() > 0) {
                String mergedFileName = cfs.getTempFileName();
                SSTable ssTable = null;
                SSTable ssTableRange = null;
                String lastkey = null;
                List<FileStruct> mergeNeeded = new ArrayList<FileStruct>();
                DataOutputBuffer bufOut = new DataOutputBuffer();
                int expectedBloomFilterSize = SSTable.getApproximateKeyCount(files);
                expectedBloomFilterSize = (expectedBloomFilterSize > 0) ? expectedBloomFilterSize : SSTable.indexInterval();
                logger_.debug("Expected bloom filter size : " + expectedBloomFilterSize);
                /* Create the bloom filter for the compacted file. */
                BloomFilter compactedBloomFilter = new BloomFilter(expectedBloomFilterSize, 8);
                BloomFilter compactedRangeBloomFilter = new BloomFilter(expectedBloomFilterSize, 8);
                List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();

                while (pq.size() > 0 || mergeNeeded.size() > 0) {
                    // pop off the queue until we get a different key
                    FileStruct fs = pq.peek();
                    if (fs != null
                        && (lastkey == null || lastkey.equals(fs.key))) {
                        // The keys are the same so we need to add this to the
                        // merge list
                        lastkey = fs.key;
                        mergeNeeded.add(pq.poll());
                        continue;
                    }

                    // merge the keys in the merge list
                    Collections.sort(mergeNeeded, new ColumnFamilyStore.FileStructComparator());
                    bufOut.reset();
                    if (mergeNeeded.size() > 1) {
                        for (FileStruct filestruct : mergeNeeded) {
                            try {
                                /* read the length although we don't need it */
                                filestruct.bufIn.readInt();
                                // Skip the Index
                                if (DatabaseDescriptor.isNameIndexEnabled(cfs.getColumnFamilyName())) {
                                    IndexHelper.skip(filestruct.bufIn);
                                }
                                // We want to add only 2 and resolve them right there in order to save on memory footprint
                                if (columnFamilies.size() > 1) {
                                    ColumnFamilyStore.merge(columnFamilies);
                                }
                                // deserialize into column families
                                columnFamilies.add(ColumnFamily.serializer().deserialize(filestruct.bufIn));
                            }
                            catch (Exception ex) {
                                ex.printStackTrace();
                                continue;
                            }
                        }
                        // Now after merging all crap append to the sstable
                        ColumnFamily columnFamily = ColumnFamilyStore.resolveAndRemoveDeleted(columnFamilies);
                        columnFamilies.clear();
                        if (columnFamily != null) {
                            /* serialize the cf with column indexes */
                            ColumnFamily.serializerWithIndexes().serialize(columnFamily, bufOut);
                        }
                    } else {
                        FileStruct filestruct = mergeNeeded.get(0);
                        try {
                            /* read the length although we don't need it */
                            int size = filestruct.bufIn.readInt();
                            bufOut.write(filestruct.bufIn, size);
                        }
                        catch (Exception ex) {
                            ex.printStackTrace();
                            filestruct.reader.close();
                            continue;
                        }
                    }
                    if (Range.isKeyInRanges(ranges, lastkey)) {
                        if (ssTableRange == null) {
                            String mergedRangeFileName = cfs.getTempFileName();
                            ssTableRange = new SSTable(DatabaseDescriptor.getBootstrapFileLocation(), mergedRangeFileName);
                        }
                        if (rangeCountingBloomFilter == null) {
                            rangeCountingBloomFilter = new CountingBloomFilter(expectedBloomFilterSize, 8);
                        }
                        ssTableRange.append(lastkey, bufOut);
                        compactedRangeBloomFilter.add(lastkey);
                        rangeCountingBloomFilter.add(lastkey);
                    } else {
                        if (ssTable == null) {
                            ssTable = new SSTable(compactionFileLocation, mergedFileName);
                        }
                        try {
                            ssTable.append(lastkey, bufOut);
                        }
                        catch (Exception ex) {
                            logger_.warn(LogUtil.throwableToString(ex));
                        }

                        /* Fill the bloom filter  with the   key */
                        compactedBloomFilter.add(lastkey);
                    }
                    totalkeysWritten++;
                    for (FileStruct filestruct : mergeNeeded) {
                        try {
                            filestruct.getNextKey();
                            if (filestruct.exhausted) {
                                continue;
                            }
                            pq.add(filestruct);
                            totalkeysRead++;
                        }
                        catch (Exception ex) {
                            // Ignore the exception as it might be a corrupted file
                            // in any case we have read as far as possible from it
                            // and it will be deleted after compaction.
                            ex.printStackTrace();
                            filestruct.reader.close();
                            continue;
                        }
                    }
                    mergeNeeded.clear();
                    lastkey = null;
                }
                if (ssTable != null) {
                    ssTable.closeRename(compactedBloomFilter);
                    newfile = ssTable.getDataFileLocation();
                }
                if (ssTableRange != null) {
                    ssTableRange.closeRename(compactedRangeBloomFilter);
                }
                totalBytesWritten = cfs.completeCompaction(files, newfile, totalBytesWritten, compactedBloomFilter);
            }
        }
        catch (Exception ex) {
            logger_.warn(LogUtil.throwableToString(ex));
        }
        logger_.debug("Total time taken for compaction  ..."
                + (System.currentTimeMillis() - startTime));
        logger_.debug("Total bytes Read for compaction  ..." + totalBytesRead);
        logger_.debug("Total bytes written for compaction  ..."
                + totalBytesWritten + "   Total keys read ..." + totalkeysRead);
        return rangeCountingBloomFilter;
    }

    static int compactionMemoryThreshold_ = 1 << 30;

    static PriorityQueue<FileStruct> initializePriorityQueue(List<String> files, List<Range> ranges, int minBufferSize) throws IOException
    {
        PriorityQueue<FileStruct> pq = new PriorityQueue<FileStruct>();
        if (files.size() > 1 || (ranges != null &&  files.size() > 0))
        {
            int bufferSize = Math.min( (compactionMemoryThreshold_ / files.size()), minBufferSize ) ;
            FileStruct fs = null;
            for (String file : files)
            {
            	try
            	{
            		fs = new FileStruct();
	                fs.bufIn = new DataInputBuffer();
	                fs.bufOut = new DataOutputBuffer();
	                fs.reader = SequenceFile.bufferedReader(file, bufferSize);
	                fs.key = null;
	                fs.getNextKey();
	                if(fs.exhausted)
	                	continue;
	                pq.add(fs);
            	}
            	catch ( Exception ex)
            	{
            		ex.printStackTrace();
            		try
            		{
            			if(fs != null)
            			{
            				fs.reader.close();
            			}
            		}
            		catch(Exception e)
            		{
            			logger_.warn("Unable to close file :" + file);
            		}
                    continue;
            	}
            }
        }
        return pq;
    }

    static class FileStruct implements Comparable<FileStruct>
    {
        IFileReader reader;
        String key;
        DataInputBuffer bufIn;
        DataOutputBuffer bufOut;
        boolean exhausted = false;

        public int compareTo(FileStruct f)
        {
            return key.compareTo(f.key);
        }

        /*
         * Read the next key from the data file , this fn will skip teh block index
         * and read teh next available key into the filestruct that is passed.
         * If it cannot read or a end of file is reached it will return null.
         */
        void getNextKey() throws IOException
        {
            bufOut.reset();
            if (reader.isEOF())
            {
                reader.close();
                exhausted = true;
                return;
            }

            long bytesread = reader.next(bufOut);
            if (bytesread == -1)
            {
                reader.close();
                exhausted = true;
                return;
            }

            bufIn.reset(bufOut.getData(), bufOut.getLength());
            key = bufIn.readUTF();
            /* If the key we read is the Block Index Key then omit and read the next key. */
            if ( key.equals(SSTable.blockIndexKey_) )
            {
                bufOut.reset();
                bytesread = reader.next(bufOut);
                if (bytesread == -1)
                {
                    reader.close();
                    exhausted = true;
                    return;
                }
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                key = bufIn.readUTF();
            }
        }
    }

    /*
     * Stage the compactions , compact similar size files.
     * This fn figures out the files close enough by size and if they
     * are greater than the threshold then compacts.
     *
     * Unused.
     */
    static Map<Integer, List<String>> stageOrderedCompaction(List<String> files)
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
     * Add up all the files sizes this is the worst case file
     * size for compaction of all the list of files given.
     */
    static long getExpectedCompactedFileSize(List<String> files)
    {
    	long expectedFileSize = 0;
    	for(String file : files)
    	{
    		File f = new File(file);
    		long size = f.length();
    		expectedFileSize = expectedFileSize + size;
    	}
    	return expectedFileSize;
    }


    /*
     *  Find the maximum size file in the list .
     */
    static String getMaxSizeFile( List<String> files )
    {
    	long maxSize = 0L;
    	String maxFile = null;
    	for ( String file : files )
    	{
    		File f = new File(file);
    		if(f.length() > maxSize )
    		{
    			maxSize = f.length();
    			maxFile = file;
    		}
    	}
    	return maxFile;
    }


    Range getMaxRange( List<Range> ranges )
    {
    	Range maxRange = new Range( BigInteger.ZERO, BigInteger.ZERO );
    	for( Range range : ranges)
    	{
    		if( range.left().compareTo(maxRange.left()) > 0 )
    		{
    			maxRange = range;
    		}
    	}
    	return maxRange;
    }

    boolean isLoopAround ( List<Range> ranges )
    {
    	boolean isLoop = false;
    	for( Range range : ranges)
    	{
    		if( range.left().compareTo(range.right()) > 0 )
    		{
    			isLoop = true;
    			break;
    		}
    	}
    	return isLoop;
    }
}