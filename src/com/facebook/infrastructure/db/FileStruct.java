package com.facebook.infrastructure.db;

import com.facebook.infrastructure.io.IFileReader;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.DataOutputBuffer;
import com.facebook.infrastructure.io.SSTable;

import java.io.IOException;

public class FileStruct implements Comparable<FileStruct>
{
    private String key = null;
    private boolean exhausted = false;
    private IFileReader reader;
    private DataInputBuffer bufIn;
    private DataOutputBuffer bufOut;

    public FileStruct(IFileReader reader) {
        this.reader = reader;
        bufIn = new DataInputBuffer();
        bufOut = new DataOutputBuffer();
    }

    public String getFileName() {
        return reader.getFileName();
    }

    public void close() throws IOException {
        reader.close();
    }

    public boolean isExhausted() {
        return exhausted;
    }

    public DataInputBuffer getBufIn() {
        return bufIn;
    }

    public String getKey() {
        return key;
    }

    public int compareTo(FileStruct f)
    {
        return key.compareTo(f.key);
    }

    // we don't use SequenceReader.seekTo, since that (sometimes) throws an exception
    // if the key is not found.  unsure if this behavior is desired.
    public void seekTo(String seekKey) {
        try {
            SSTable.Range range = SSTable.getRange(seekKey, reader);
            reader.seek(range.end);
            long position = reader.getPositionFromBlockIndex(seekKey);
            if (position == -1) {
                reader.seek(range.start);
            } else {
                reader.seek(position);
            }

            while (!exhausted) {
                getNextKey();
                if (key.compareTo(seekKey) >= 0) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("corrupt sstable", e);
        }
    }

    /*
     * Read the next key from the data file, skipping block indexes.
     * Caller must check isExhausted after each call to see if further
     * reads are valid.
     */
    public void getNextKey() throws IOException
    {
        if (exhausted) {
            return;
        }

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
