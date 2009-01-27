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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Adler32;
import org.apache.log4j.Logger;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.FileUtils;
import com.facebook.infrastructure.utils.LogUtil;
import bak.pcj.map.AbstractLongKeyLongMap;
import bak.pcj.map.LongKeyLongChainedHashMap;

/**
 * This class manages the persistence of checksums and keeps
 * them in memory. It maintains a mapping of data files on
 * disk to their corresponding checksum files. It is also
 * loads the checksums in memory on start up.
 * 
 * @author alakshman
 *
 */
class ChecksumManager
{    
    private static Logger logger_ = Logger.getLogger(ChecksumManager.class);
    /* Keeps a mapping of checksum manager instances to data file */
    private static Map<String, ChecksumManager> chksumMgrs_ = new HashMap<String, ChecksumManager>();
    private static Lock lock_ = new ReentrantLock();
    private static final String checksumPrefix_ = "Checksum-";
    private static final int bufferSize_ = 8*1024*1024;
    private static final long chunkMask_ = 0x00000000FFFFFFFFL;
    private static final long fileIdMask_ = 0x7FFFFFFF00000000L;
    /* Map where checksums are cached. */
    private static AbstractLongKeyLongMap chksums_ = new LongKeyLongChainedHashMap();

    public static ChecksumManager instance(String dataFile) throws IOException
    {
        ChecksumManager chksumMgr = chksumMgrs_.get(dataFile);
        if ( chksumMgr == null )
        {
            lock_.lock();
            try
            {
                if ( chksumMgr == null )
                {
                    chksumMgr = new ChecksumManager(dataFile);
                    chksumMgrs_.put(dataFile, chksumMgr);
                }
            }
            finally
            {
                lock_.unlock();
            }
        }
        return chksumMgr;
    }
    
    /**
     * On start read all the check sum files on disk and
     * pull them into memory.
     * @throws IOException
     */
    public static void onStart() throws IOException
    {
        String[] directories = DatabaseDescriptor.getAllDataFileLocations(); 
        List<File> allFiles = new ArrayList<File>();
        for ( String directory : directories )
        {
            File file = new File(directory);
            File[] files = file.listFiles();
            for ( File f : files )
            {
                if ( f.getName().contains(ChecksumManager.checksumPrefix_) )
                {
                    allFiles.add(f);
                }
            }
        }
        
        for ( File file : allFiles )
        {                           
            int fId = SequenceFile.getFileId(file.getName());
            ChecksumReader chksumRdr = new ChecksumReader(file.getAbsolutePath(), 0L, file.length());
                        
            int chunk = 0;
            while ( !chksumRdr.isEOF() )
            {
                long value = chksumRdr.readLong();
                long key = ChecksumManager.key(fId, ++chunk);
                chksums_.put(key, value);
            }
        }
    }
    
    /**
     * On delete of this dataFile remove the checksums associated with
     * this file from memory, remove the check sum manager instance.
     * 
     * @param dataFile data file that is being deleted.
     * @throws IOException
     */
    public static void onFileDelete(String dataFile) throws IOException
    {
        File f = new File(dataFile);
        long size = f.length();
        int fileId = SequenceFile.getFileId(f.getName());
        int chunks = (int)(size >> 16L);
        
        for ( int i = 0; i < chunks; ++i )
        {
            long key = ChecksumManager.key(fileId, i);
            chksums_.remove(key);
        }
        
        /* remove the check sum manager instance */
        chksumMgrs_.remove(dataFile);
        String chksumFile = f.getParent() + System.getProperty("file.separator") + checksumPrefix_ + fileId + ".db";
        FileUtils.delete(chksumFile);
    }
    
    private static long key(int fileId, int chunkId)
    {
        long key = 0;
        key |= fileId;
        key <<= 32;
        key |= chunkId;
        return key;
    }
    
    private RandomAccessFile raf_;
    private Adler32 adler_ = new Adler32();
    
    ChecksumManager(String dataFile) throws IOException
    {
        File file = new File(dataFile);
        String directory = file.getParent();
        String f = file.getName();
        short fId = SequenceFile.getFileId(f);
        String chkSumFile = directory + System.getProperty("file.separator") + checksumPrefix_ + fId + ".db";
        raf_ = new RandomAccessFile(chkSumFile, "rw");
    }
    
    /**
     * Log the checksum for the the specified file and chunk
     * within the file.
     * @param fileId id associated with the file
     * @param chunkId chunk within the file.
     * @param chksum calculated checksum for the chunk
     * @throws IOException
     */
    void logChecksum(int fileId, int chunkId, byte[] buffer)
    {
        try
        {
            adler_.update(buffer);
            long chksum = adler_.getValue();
            adler_.reset();
            /* log checksums to disk */
            raf_.writeLong(chksum);
            /* add the chksum to memory */
            long key = ChecksumManager.key(fileId, chunkId);
            chksums_.put(key, chksum);
        }
        catch ( IOException ex )
        {
            logger_.warn( LogUtil.throwableToString(ex) );
        }
    }
    
    /**
     * Validate checksums for the data in the buffer for the region
     * that is encapsulated in the section object
     * @param bufOut buffer containing the data.
     * @param section coordinates indicating the region on disk.
     * @throws IOException
     */
    void validateChecksum(byte[] buffer, int chunkId, String file) throws IOException
    {                
        int fId = SequenceFile.getFileId(file);
        long key = ChecksumManager.key(fId, chunkId);
        adler_.update(buffer);
        long currentChksum = adler_.getValue();
        adler_.reset();
        long oldChksum = chksums_.get(key);
        if ( currentChksum != oldChksum )
        {            
            System.out.println("EXCEPTION:" + chunkId);
            throw new IOException("Checksums do not match for this chunk.");
        }
        else
        {
            System.out.println(chunkId);
        }
    }
    
    /**
     * Get the checksum for the specified file's chunk
     * @param fileId id associated with the file.
     * @param chunkId chunk within the file.
     * @return associated checksum for the chunk
     */
    long getChecksum(int fileId, int chunkId)
    {        
        long key = ChecksumManager.key(fileId, chunkId);
        return chksums_.get(key);
    }
    
    public static void main(String[] args) throws Throwable
    {
        ChecksumReader rdr = new ChecksumReader("C:\\Engagements\\Cassandra\\Checksum-1.db");
        while ( !rdr.isEOF() )
        {
            System.out.println(rdr.readLong());
        }
        rdr.close();
    }
}

/**
 * ChecksumReader is used to memory map the checksum files and
 * load the data into memory.
 * 
 * @author alakshman
 *
 */
class ChecksumReader 
{
    private static Logger logger_ = Logger.getLogger(ChecksumReader.class);
    private String filename_;
    private MappedByteBuffer buffer_;

    ChecksumReader(String filename) throws IOException
    {
        filename_ = filename;
        File f = new File(filename);
        map(0, f.length());
    }
    
    ChecksumReader(String filename, long start, long end) throws IOException
    {        
        filename_ = filename;
        map(start, end);
    }

    public void map() throws IOException
    {
        RandomAccessFile file = new RandomAccessFile(filename_, "rw");
        try
        {
            buffer_ = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length() );
            buffer_.load();
        }
        finally
        {
            file.close();
        }
    }

    public void map(long start, long end) throws IOException
    {
        if ( start < 0 || end < 0 || end < start )
            throw new IllegalArgumentException("Invalid values for start and end.");

        RandomAccessFile file = new RandomAccessFile(filename_, "rw");
        try
        {
            if ( end == 0 )
                end = file.length();
            buffer_ = file.getChannel().map(FileChannel.MapMode.READ_ONLY, start, end);
            buffer_.load();
        }
        finally
        {
            file.close();
        }
    }

    void unmap(final Object buffer)
    {
        AccessController.doPrivileged( new PrivilegedAction<MappedByteBuffer>()
                {
            public MappedByteBuffer run()
            {
                try
                {
                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
                    getCleanerMethod.setAccessible(true);
                    sun.misc.Cleaner cleaner = (sun.misc.Cleaner)getCleanerMethod.invoke(buffer,new Object[0]);
                    cleaner.clean();
                }
                catch(Throwable e)
                {
                    logger_.debug( LogUtil.throwableToString(e) );
                }
                return null;
            }
                });
    }
    
    public long readLong() throws IOException
    {
        return buffer_.getLong();
    }
    
    public boolean isEOF()
    {
        return ( buffer_.remaining() == 0 );
    }

    
    public void close() throws IOException
    {
        unmap(buffer_);
    }
}
