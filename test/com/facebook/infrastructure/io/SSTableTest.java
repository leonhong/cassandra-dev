package com.facebook.infrastructure.io;

import com.facebook.infrastructure.ServerTest;
import com.facebook.infrastructure.db.FileStruct;
import com.facebook.infrastructure.utils.BloomFilter;
import org.apache.commons.collections.CollectionUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SSTableTest extends ServerTest {
    @Test
    public void testSingleWrite() throws IOException {
        File f = File.createTempFile("sstable", "");
        SSTable ssTable;

        // write test data
        ssTable = new SSTable(f.getParent(), f.getName());
        BloomFilter bf = new BloomFilter(1000, 8);
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);

        String key = Integer.toString(1);
        ssTable.append(key, bytes);
        bf.add(key);
        ssTable.close(bf);

        // verify
        ssTable = new SSTable(f.getPath() + "-Data.db");
        DataInputBuffer bufIn = ssTable.next(key, "Test:C");
        byte[] bytes2 = new byte[1024];
        bufIn.readFully(bytes2);
        assert Arrays.equals(bytes2, bytes);
    }

    @Test
    public void testManyWrites() throws IOException {
        File f = File.createTempFile("sstable", "");
        SSTable ssTable;

        TreeMap<String, byte[]> map = new TreeMap<String,byte[]>();
        for ( int i = 100; i < 1000; ++i )
        {
            map.put(Integer.toString(i), ("Avinash Lakshman is a good man: " + i).getBytes());
        }

        // write
        ssTable = new SSTable(f.getParent(), f.getName());
        BloomFilter bf = new BloomFilter(1000, 8);
        for (String key: map.navigableKeySet())
        {
            ssTable.append(key, map.get(key));
        }
        ssTable.close(bf);

        // verify
        List<String> keys = new ArrayList(map.keySet());
        Collections.shuffle(keys);
        ssTable = new SSTable(f.getPath() + "-Data.db");
        for (String key: keys)
        {
            DataInputBuffer bufIn = ssTable.next(key, "Test:C");
            byte[] bytes2 = new byte[map.get(key).length];
            bufIn.readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key));
        }
    }

    @Test
    public void testFileStruct() throws IOException {
        File f = File.createTempFile("sstable", "");
        SSTable ssTable;

        TreeMap<String, byte[]> map = new TreeMap<String,byte[]>();
        for ( int i = 200; i < 1000; i += 2 )
        {
            map.put(Integer.toString(i), ("Avinash Lakshman is a good man: " + i).getBytes());
        }

        // write
        ssTable = new SSTable(f.getParent(), f.getName());
        BloomFilter bf = new BloomFilter(1000, 8);
        for (String key: map.navigableKeySet())
        {
            ssTable.append(key, map.get(key));
        }
        ssTable.close(bf);

        List<String> keys = new ArrayList<String>();
        CollectionUtils.addAll(keys, new ArrayList(map.keySet()).listIterator(200));
        assert keys.get(0).equals("600");
        FileStruct fs = new FileStruct(SequenceFile.reader(f.getPath() + "-Data.db"));
        fs.seekTo("599");
        assert fs.getKey().equals("600");
        for (String key : keys) {
            assert !fs.isExhausted();
            assert key.equals(fs.getKey());
            fs.getNextKey();
        }
        assert fs.isExhausted();
    }
}
