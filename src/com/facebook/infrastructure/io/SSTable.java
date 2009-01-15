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

package com.facebook.infrastructure.io;

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.utils.BasicUtilities;
import com.facebook.infrastructure.utils.BloomFilter;
import com.facebook.infrastructure.utils.LogUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * This class is built on top of the SequenceFile. It stores
 * data on disk in sorted fashion. However the sorting is upto
 * the application. This class expects keys to be handed to it
 * in sorted order. SSTable is broken up into blocks where each
 * block contains 128 keys. At the end of every block the block 
 * index is written which contains the offsets to the keys in the
 * block. SSTable also maintains an index file to which every 128th 
 * key is written with a pointer to the block index which is the block 
 * that actually contains the key. This index file is then read and 
 * maintained in memory. SSTable is append only and immutable. SSTable
 * on disk looks as follows:
 * 
 *                 -------------------------
 *                 |------------------------|<-------|
 *                 |                        |        |  BLOCK-INDEX PTR
 *                 |                        |        |
 *                 |------------------------|--------
 *                 |------------------------|<-------|
 *                 |                        |        |
 *                 |                        |        |  BLOCK-INDEX PTR 
 *                 |                        |        |
 *                 |------------------------|---------
 *                 |------------------------|<--------|
 *                 |                        |         |
 *                 |                        |         |
 *                 |                        |         | BLOCK-INDEX PTR
 *                 |                        |         |
 *                 |------------------------|         |
 *                 |------------------------|----------
 *                 |------------------------|-----------------> BLOOM-FILTER
 * version-info <--|----------|-------------|-------> relative offset to last block index.
 *                 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class SSTable
{
    private static Logger logger_ = Logger.getLogger(SSTable.class);
    /* use this as a monitor to lock when loading index. */
    private static Object indexLoadLock_ = new Object();
    /* Every 128th key is an index. */
    private static final int indexInterval_ = 128;
    /* Key associated with block index written to disk */
    public static final String blockIndexKey_ = "BLOCK-INDEX";
    /* Position in SSTable after the first Block Index */
    private static long positionAfterFirstBlockIndex_ = 0L;
    /* Required extension for temporary files created during compactions. */
    public static final String temporaryFile_ = "tmp";
    /* Use this long as a 64 bit entity to turn on some bits for various settings */
    private static final long version_ = 0L;
    /*
     * This map has the SSTable as key and a BloomFilter as value. This
     * BloomFilter will tell us if a key/column pair is in the SSTable.
     * If not we can avoid scanning it.
     */
    private static Map<String, BloomFilter> bfs_ = new Hashtable<String, BloomFilter>();
    /* Maintains a touched set of keys */
    private static LinkedHashMap<String, Long> touchCache_ = new TouchedKeyCache(DatabaseDescriptor.getTouchKeyCacheSize());

    /**
     * This class holds the position of a key in a block
     * and the size of the data associated with this key. 
    */
    static class BlockMetadata
    {
        static final BlockMetadata NULL = new BlockMetadata(-1L, -1L);
        
        long position_;
        long size_;
        
        BlockMetadata(long position, long size)
        {
            position_ = position;
            size_ = size;
        }
    }
    
    /*
     * This abstraction provides LRU symantics for the keys that are 
     * "touched". Currently it holds the offset of the key in a data
     * file. May change to hold a reference to a IFileReader which
     * memory maps the key and its associated data on a touch.
    */
    private static class TouchedKeyCache extends LinkedHashMap<String, Long>
    {
        private final int capacity_;
        
        TouchedKeyCache(int capacity)
        {
            super(capacity + 1, 1.1f, true);
            capacity_ = capacity;
        }
        
        protected boolean removeEldestEntry(Map.Entry<String, Long> entry)
        {
            return ( size() > capacity_ );
        }
    }

    /**
     * This is a simple container for the index Key and its corresponding position
     * in the data file. Binary search is performed on a list of these objects
     * to lookup keys within the SSTable data file.
    */
    public static class KeyPositionInfo implements Comparable<KeyPositionInfo>
    {
        public final String key;
        public final long position;

        public KeyPositionInfo(String key)
        {
            this(key, 0);
        }

        public KeyPositionInfo(String key, long position)
        {
            this.key = key;
            this.position = position;
        }

        public int compareTo(KeyPositionInfo kPosInfo)
        {
            return key.compareTo(kPosInfo.key);
        }

        public String toString()
        {
        	return key + ":" + position;
        }
    }
    
    public static int indexInterval()
    {
    	return indexInterval_;
    }
    
    /*
     * Maintains a list of KeyPositionInfo objects per SSTable file loaded.
     * We do this so that we don't read the index file into memory multiple
     * times.
    */
    private static Map<String, List<KeyPositionInfo>> indexMetadataMap_ = new Hashtable<String, List<KeyPositionInfo>>();
    
    /** 
     * This method deletes both the specified data file
     * and the associated index file
     *
     * @param dataFile - data file associated with the SSTable
    */
    public static void delete(String dataFile)
    {        
        /* remove the cached index table from memory */
        indexMetadataMap_.remove(dataFile);
        
        File file = new File(dataFile);
        if ( file.exists() )
            /* delete the data file */
			if (file.delete())
			{			    
			    logger_.info("** Deleted " + file.getName() + " **");
			}
			else
			{			  
			    logger_.error("Failed to delete " + file.getName());
			}
    }

    public static int getApproximateKeyCount( List<String> dataFiles)
    {
    	int count = 0 ;

    	for(String dataFile : dataFiles )
    	{    		
    		List<KeyPositionInfo> index = indexMetadataMap_.get(dataFile);
    		if (index != null )
    		{
                count += index.size() + 1;
    		}
    	}

    	return count * indexInterval_;
    }

    /**
     * Get all indexed keys in the SSTable.
    */
    public static List<String> getSortedKeys()
    {
        Set<String> indexFiles = indexMetadataMap_.keySet();
        List<KeyPositionInfo> keyPositionInfos = new ArrayList<KeyPositionInfo>();

        for ( String indexFile : indexFiles )
        {
            keyPositionInfos.addAll( indexMetadataMap_.get(indexFile) );
        }

        List<String> indexedKeys = new ArrayList<String>();
        for ( KeyPositionInfo keyPositionInfo : keyPositionInfos )
        {
            indexedKeys.add(keyPositionInfo.key);
        }

        Collections.sort(indexedKeys);
        return indexedKeys;
    }
    
    public static void onStart(List<String> filenames) throws IOException
    {
        for ( String filename : filenames )
        {
            SSTable.maybeLoadIndexFile(filename);
        }
    }

    /*
     * Stores the Bloom Filter associated with the given file.
    */
    public static void storeBloomFilter(String filename, BloomFilter bf)
    {
        bfs_.put(filename, bf);
    }

    /*
     * Removes the bloom filter associated with the specified file.
    */
    public static void removeAssociatedBloomFilter(String filename)
    {
        bfs_.remove(filename);
    }

    /*
     * Determines if the given key is in the specified file. If the
     * key is not present then we skip processing this file.
    */
    public static boolean isKeyInFile(String key, String filename)
    {
        boolean bVal = false;
        BloomFilter bf = bfs_.get(filename);
        if ( bf != null )
        {
            bVal = bf.isPresent(key);
        }
        return bVal;
    }
    
    public static long fetchOffset(String key, String file) throws IOException
    {
        long position = -1L;
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        IFileReader dataReader = SequenceFile.bufferedReader(file, 1024*1024);        
        
        while ( !dataReader.isEOF() )
        {
            bufOut.reset();                
            /* Record the position of the key. */
            position = dataReader.getCurrentPosition();
            dataReader.next(bufOut);
            bufIn.reset(bufOut.getData(), bufOut.getLength());
            /* Key just read */
            String keyOnDisk = bufIn.readUTF();
            if ( keyOnDisk.equals(key) )
            {
                break;
            }
        }
        return position;
    }

    private String dataFile_;    
    private IFileWriter dataWriter_;
    private String lastWrittenKey_;
    private long prevBlockPosition_ = 0L;    
    private int indexKeysWritten_ = 0;
    /* Holds the keys and their respective positions in a block */
    private SortedMap<String, BlockMetadata> blockIndex_ = new TreeMap<String, BlockMetadata>(Collections.reverseOrder());

    /*
     * This ctor basically gets passed in the full path name
     * of the data file associated with this SSTable. Use this
     * ctor to read the data in this file.
    */
    public SSTable(String dataFileName) throws IOException
    {        
        dataFile_ = dataFileName;
        SSTable.maybeLoadIndexFile(dataFile_);
    }

    /*
     * Intialize the index files and also cache the Bloom Filters
     * associated with these files.
    */
    public static void maybeLoadIndexFile(String filename) throws IOException {
        // prevent multiple threads from loading the same index files multiple times
        synchronized( indexLoadLock_ )
        {
            if ( indexMetadataMap_.get(filename) == null )
            {
                long start = System.currentTimeMillis();
                loadIndexFile(filename);
                logger_.debug("INDEX LOAD TIME: " + (System.currentTimeMillis() - start) + " ms.");
            }
        }
    }

    private static void loadBloomFilter(IFileReader reader, long size) throws IOException
    {
        /* read the position of the bloom filter */
        reader.seek(size - 8);
        byte[] bytes = new byte[8];
        long currentPosition = reader.getCurrentPosition();
        reader.readDirect(bytes);
        long position = BasicUtilities.byteArrayToLong(bytes);
        /* seek to the position of the bloom filter */
        reader.seek(currentPosition - position);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        /* read the bloom filter from disk */
        reader.next(bufOut);
        bufIn.reset(bufOut.getData(), bufOut.getLength());
        String key = bufIn.readUTF();
        if ( key.equals(SequenceFile.marker_) )
        {
            /*
             * We are now reading the serialized Bloom Filter. We read
             * the length and then pass the bufIn to the serializer of
             * the BloomFilter. We then store the Bloom filter in the
             * map. However if the Bloom Filter already exists then we
             * need not read the rest of the file.
            */
            bufIn.readInt();
            if ( bfs_.get(reader.getFileName()) == null )
                bfs_.put(reader.getFileName(), BloomFilter.serializer().deserialize(bufIn));
        }
    }

    private static void loadIndexFile(String filename) throws IOException
    {
        IFileReader indexReader = SequenceFile.reader(filename);
        File file = new File(filename);
        long size = file.length();
        /* load the bloom filter into memory */
        loadBloomFilter(indexReader, size);
        /* read the position of the last block index */
        byte[] bytes = new byte[8];
        /* seek to the position to read the relative position of the last block index */
        indexReader.seek(size - 16L);
        /* the beginning of the last block index */
        long currentPosition = indexReader.getCurrentPosition();
        indexReader.readDirect(bytes);
        long lastBlockIndexPosition = BasicUtilities.byteArrayToLong(bytes);
        List<KeyPositionInfo> keyPositionInfos = new ArrayList<KeyPositionInfo>();
        indexMetadataMap_.put(filename, keyPositionInfos);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        /* Read all block indexes to maintain an index in memory */
        try
        {
            long nextPosition = currentPosition - lastBlockIndexPosition;
            /* read the block indexes from the end of the file till we hit the first one. */
            while ( nextPosition > 0 )
            {
                indexReader.seek(nextPosition);
                bufOut.reset();
                /* position @ the current block index being processed */
                currentPosition = indexReader.getCurrentPosition();
                long bytesRead = indexReader.next(bufOut);
                if ( bytesRead != -1 )
                {
                    bufIn.reset(bufOut.getData(), bufOut.getLength());
                    /* read the block key. */
                    String blockIndexKey = bufIn.readUTF();
                    if ( !blockIndexKey.equals(SSTable.blockIndexKey_) )
                        throw new IOException("Unexpected position to be reading the block index from.");
                    /* read the size of the block index */
                    int sizeOfBlockIndex = bufIn.readInt();
                    /* Number of keys in the block. */
                    int keys = bufIn.readInt();
                    String largestKeyInBlock = null;
                    for ( int i = 0; i < keys; ++i )
                    {
                        String keyInBlock = bufIn.readUTF();
                        if ( i == 0 )
                        {
                            largestKeyInBlock = keyInBlock;
                            /* relative offset in the block for the key*/
                            long position = bufIn.readLong();
                            /* size of data associated with the key */
                            bufIn.readLong();
                            /* load the actual position of the block index into the index map */
                            keyPositionInfos.add( new KeyPositionInfo(largestKeyInBlock, currentPosition) );
                        }
                        else
                        {
                            /*
                             * This is not the key we are looking for. So read its position
                             * and the size of the data associated with it. This was stored
                             * as the BlockMetadata.
                            */
                            long position = bufIn.readLong();
                            bufIn.readLong();
                        }
                    }
                    lastBlockIndexPosition = bufIn.readLong();
                    nextPosition = currentPosition - lastBlockIndexPosition;
                }
            }
            Collections.sort(keyPositionInfos);
        }
        catch( IOException ex )
        {
        	logger_.error(LogUtil.throwableToString(ex));
        }
        finally
        {
            indexReader.close();
        }
    }

  //
//
// BEGIN ACTUAL SSTABLE CODE
//
 //

    /*
     * This ctor is used for writing data into the SSTable. Use this
     * version to write to the SSTable.
    */
    public SSTable(String directory, String filename) throws IOException
    {        
        dataFile_ = directory + System.getProperty("file.separator") + filename + "-Data.db";
        dataWriter_ = SequenceFile.bufferedWriter(dataFile_, 32*1024*1024);
        // dataWriter_ = SequenceFile.checksumWriter(dataFile_);
        /* Write the block index first. This is an empty one */
        dataWriter_.append(SSTable.blockIndexKey_, new byte[0]);
        SSTable.positionAfterFirstBlockIndex_ = dataWriter_.getCurrentPosition();
    }

    public String getDataFileLocation() throws IOException
    {
        File file = new File(dataFile_);
        if ( file.exists() )
            return file.getAbsolutePath();
        throw new IOException("File " + dataFile_ + " was not found on disk.");
    }

    public long lastModified()
    {
        return dataWriter_.lastModified();
    }
    
    /*
     * Seeks to the specified key on disk.
    */
    public void touch(String key, boolean fData) throws IOException
    {
        if ( touchCache_.containsKey(key) )
            return;
        
        IFileReader dataReader = SequenceFile.reader(dataFile_); 
        try
        {
            Range fileCoordinate = getRange(key, dataReader);
            /* Get offset of key from block Index */
            dataReader.seek(fileCoordinate.end);
            BlockMetadata blockMetadata = dataReader.getBlockMetadata(key);
            if ( blockMetadata.position_ != -1L )
            {
                touchCache_.put(dataFile_ + ":" + key, blockMetadata.position_);                  
            } 
            
            if ( fData )
            {
                /* Read the data associated with this key and pull it into the Buffer Cache */
                if ( blockMetadata.position_ != -1L )
                {
                    dataReader.seek(blockMetadata.position_);
                    DataOutputBuffer bufOut = new DataOutputBuffer();
                    dataReader.next(bufOut);
                    bufOut.reset();
                    logger_.debug("Finished the touch of the key to pull it into buffer cache.");
                }
            }
        }
        finally
        {
            if ( dataReader != null )
                dataReader.close();
        }
    }

    private long beforeAppend(String key) throws IOException
    {
    	if(key == null )
            throw new IOException("Keys must not be null.");
        if ( lastWrittenKey_ != null && key.compareTo(lastWrittenKey_) <= 0 )
        {
            logger_.info("Last written key : " + lastWrittenKey_);
            logger_.info("Current key : " + key);
            logger_.info("Writing into file " + dataFile_);
            throw new IOException("Keys must be written in ascending order.");
        }
        long currentPosition = (lastWrittenKey_ == null) ? SSTable.positionAfterFirstBlockIndex_ : dataWriter_.getCurrentPosition();
        return currentPosition;
    }

    private void afterAppend(String key, long position, long size) throws IOException
    {
        ++indexKeysWritten_;
        lastWrittenKey_ = key;
        blockIndex_.put(key, new BlockMetadata(position, size));
        if ( indexKeysWritten_ == indexInterval_ )
        {
            dumpBlockIndex();        	
            indexKeysWritten_ = 0;
        }                
    }
    
    private void dumpBlockIndex() throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        /* 
         * Record the position where we start writing the block index. This is will be
         * used as the position of the lastWrittenKey in the block in the index file
        */
        long position = dataWriter_.getCurrentPosition();
        Set<String> keys = blockIndex_.keySet();                
        /* Number of keys in this block */
        bufOut.writeInt(keys.size());
        for ( String key : keys )
        {            
            bufOut.writeUTF(key);
            BlockMetadata blockMetadata = blockIndex_.get(key);
            /* position of the key as a relative offset */
            bufOut.writeLong(position - blockMetadata.position_);
            bufOut.writeLong(blockMetadata.size_);
        }
                
        /* Write the relative offset to the previous block index. */
        bufOut.writeLong(position - prevBlockPosition_);
        prevBlockPosition_ = position;
        /* Write out the block index. */
        dataWriter_.append(SSTable.blockIndexKey_, bufOut);
        blockIndex_.clear();        
        /* Load this index into the in memory index map */
        List<KeyPositionInfo> keyPositionInfos = SSTable.indexMetadataMap_.get(dataFile_);
        if ( keyPositionInfos == null )
        {
        	keyPositionInfos = new ArrayList<KeyPositionInfo>();
        	SSTable.indexMetadataMap_.put(dataFile_, keyPositionInfos);
        }
        keyPositionInfos.add(new KeyPositionInfo(lastWrittenKey_, position));
    }

    public void append(String key, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(key);
        dataWriter_.append(key, buffer);
        afterAppend(key, currentPosition, buffer.getLength());
    }

    public void append(String key, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(key);
        dataWriter_.append(key, value);
        afterAppend(key, currentPosition, value.length );
    }

    private Range getRange(String key, IFileReader dataReader) throws IOException
    {
    	List<KeyPositionInfo> indexInfo = indexMetadataMap_.get(dataFile_);
    	int size = (indexInfo == null) ? 0 : indexInfo.size();
    	long start = 0L;
    	long end = dataReader.getEOF();
        if ( size > 0 )
        {
            final int index = Collections.binarySearch(indexInfo, new KeyPositionInfo(key));
            if ( index < 0 )
            {
                // key is not present at all; scan is required
                int insertIndex = (index + 1) * -1;
                start = (insertIndex == 0) ? 0 : indexInfo.get(insertIndex - 1).position;
                if ( insertIndex < size )
                {
                    end = indexInfo.get(insertIndex).position;
                }
                else
                {
                    /* This is the Block Index in the file. */
                    end = start;
                }
            }
            else
            {
                /* If we are here that means the key is in the index file
                 * and we can retrieve it w/o a scan.
                 * TODO we would
                 * like to have a retreive(key, fromPosition) but for now
                 * we use scan(start, start + 1) - a hack. */
                start = indexInfo.get(index).position;
                end = start;
            }
        }
        else
        {
            /*
             * We are here which means there are less than
             * 128 keys in the system and hence our only recourse
             * is a linear scan from start to finish. Automatically
             * use memory mapping since we have a huge file and very
             * few keys.
            */
            end = dataReader.getEOF();
        }  
        
        return new Range(start, end);
    }
    
    public DataInputBuffer next(String key, String cf, List<String> cNames) throws IOException
    {
    	DataInputBuffer bufIn = null;
        IFileReader dataReader = SequenceFile.reader(dataFile_);
        try
        {
        	Range fileCoordinate = getRange(key, dataReader);

            /*
             * we have the position we have to read from in order to get the
             * column family, get the column family and column(s) needed.
            */        	
            bufIn = getData(dataReader, key, cf, cNames, fileCoordinate);
        }
        finally
        {
            if ( dataReader != null )
                dataReader.close();
        }
        return bufIn;
    }
    
    public DataInputBuffer next(String key, String columnName) throws IOException
    {
        DataInputBuffer bufIn = null;
        IFileReader dataReader = SequenceFile.reader(dataFile_);
        //IFileReader dataReader = SequenceFile.checksumReader(dataFile_);
        
        try
        {
        	Range fileCoordinate = getRange(key, dataReader);
            /*
             * we have the position we have to read from in order to get the
             * column family, get the column family and column(s) needed.
            */            
            bufIn = getData(dataReader, key, columnName, fileCoordinate);
        }
        finally
        {
            if ( dataReader != null )
                dataReader.close();
        }
        return bufIn;
    }
    
    long getSeekPosition(String key, long start)
    {
        Long seekStart = touchCache_.get(dataFile_ + ":" + key);
        if( seekStart != null)
        {
            return seekStart;
        }
        return start;
    }
        
    /*
     * Get the data for the key from the position passed in. 
    */
    private DataInputBuffer getData(IFileReader dataReader, String key, String column, Range section) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
                
        try
        {
            dataReader.next(key, bufOut, column, section);
            if ( bufOut.getLength() > 0 )
            {                              
                bufIn.reset(bufOut.getData(), bufOut.getLength());            
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();            
            }
        }
        catch ( IOException ex )
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
        return bufIn;
    }
    
    private DataInputBuffer getData(IFileReader dataReader, String key, String cf, List<String> columns, Range section) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
                  
        try
        {
            dataReader.next(key, bufOut, cf, columns, section);            
            if ( bufOut.getLength() > 0 )
            {                     
                bufIn.reset(bufOut.getData(), bufOut.getLength());             
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();            
            }
        }
        catch( IOException ex )
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
        return bufIn;
    }

    public void close(BloomFilter bf) throws IOException
    {
        /* Any remnants in the blockIndex should be dumped */
        dumpBlockIndex();
    	/* reset the buffer and serialize the Bloom Filter. */
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, bufOut);
        byte[] bytes = new byte[bufOut.getLength()];
        System.arraycopy(bufOut.getData(), 0, bytes, 0, bytes.length);
        /*
         * Write the bloom filter for this SSTable.
         * Then write two longs one which is a version
         * and one which is a pointer to the last written
         * block index.
         */
        long bloomFilterPosition = dataWriter_.getCurrentPosition();
        dataWriter_.close(bytes, bytes.length);
        /* write the version field into the SSTable */
        dataWriter_.writeDirect(BasicUtilities.longToByteArray(version_));
        /* write the relative position of the last block index from current position */
        long blockPosition = dataWriter_.getCurrentPosition() - prevBlockPosition_;
        dataWriter_.writeDirect(BasicUtilities.longToByteArray(blockPosition));
        /* write the position of the bloom filter */
        long bloomFilterRelativePosition = dataWriter_.getCurrentPosition() - bloomFilterPosition;
        dataWriter_.writeDirect(BasicUtilities.longToByteArray(bloomFilterRelativePosition));
        dataWriter_.close();
        bufOut.close();
    }

    /*
     * Renames a temporray sstable file to a valid data and index file
     */
    public void closeRename(BloomFilter bf) throws IOException
    {
    	close( bf);
        String tmpDataFile = dataFile_;
    	String dataFileName = dataFile_.replace("-" + temporaryFile_,"");    	
    	File dataFile = new File(dataFile_);
    	dataFile.renameTo(new File(dataFileName));    	    	
    	dataFile_ = dataFileName;
    	/* Now repair the in memory index associated with the old name */
    	List<KeyPositionInfo> keyPositionInfos = SSTable.indexMetadataMap_.remove(tmpDataFile);    	    	  	    	
    	SSTable.indexMetadataMap_.put(dataFile_, keyPositionInfos);
    }
    
    public void closeRename(BloomFilter bf, List<String> files) throws IOException
    {
        close( bf);
        String tmpDataFile = dataFile_;
        String dataFileName = dataFile_.replace("-" + temporaryFile_,"");
        File dataFile = new File(dataFile_);
        dataFile.renameTo(new File(dataFileName));
        dataFile_ = dataFileName;
        /* Now repair the in memory index associated with the old name */
        List<KeyPositionInfo> keyPositionInfos = SSTable.indexMetadataMap_.remove(tmpDataFile);                         
        SSTable.indexMetadataMap_.put(dataFile_, keyPositionInfos);
        if ( files != null )
        {            
            files.add(dataFile_);
        }
    }

    /**
     * Section of a file that needs to be scanned
     */
    static class Range
    {
        long start;
        long end;

        Range(long start, long end)
        {
            this.start = start;
            this.end = end;
        }
    }
}
