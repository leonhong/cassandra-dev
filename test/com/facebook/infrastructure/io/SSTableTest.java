package com.facebook.infrastructure.io;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Collection;
import java.util.Arrays;

import com.facebook.infrastructure.utils.BloomFilter;
import com.facebook.infrastructure.db.ColumnFamily;
import com.facebook.infrastructure.db.IColumn;

public class SSTableTest {
    @Test
    public void testOne() throws IOException {
        File f = File.createTempFile("sstable", "");
        ColumnFamily cf;
        SSTable ssTable;

        // write test data
        ssTable = new SSTable(f.getParent(), f.getName());
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter bf = new BloomFilter(1000, 8);
        Random random = new Random();
        byte[] bytes = new byte[1024*1024];
        random.nextBytes(bytes);

        String key = Integer.toString(1);
        cf = new ColumnFamily("Test", "Standard");
        bufOut.reset();
        cf.createColumn("C", bytes, 1);
        ColumnFamily.serializer2().serialize(cf, bufOut);
        ssTable.append(key, bufOut);
        bf.fill(key);

        ssTable.close(bf);

        // verify
        ssTable = new SSTable(f.getPath() + "-Data.db");
        DataInputBuffer bufIn = ssTable.next(key, "Test:C");
        cf = ColumnFamily.serializer().deserialize(bufIn);
        assert cf != null;
        assert cf.name().equals("Test");
        assert cf.getAllColumns().size() == 1;
        assert Arrays.equals(cf.getColumn("C").value(), bytes);
    }

    @Test
    public void testTwo() throws IOException {
        File f = File.createTempFile("sstable", "");
        ColumnFamily cf;
        SSTable ssTable;

        // write
        ssTable = new SSTable(f.getParent(), f.getName());
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter bf = new BloomFilter(1000, 8);
        byte[] bytes = new byte[64*1024];
        Random random = new Random();
        for ( int i = 100; i < 1000; ++i )
        {
            String key = Integer.toString(i);
            cf = new ColumnFamily("Test", "Standard");
            bufOut.reset();
            cf.createColumn("C", ("Avinash Lakshman is a good man: " + i).getBytes(), i);
            ColumnFamily.serializer2().serialize(cf, bufOut);
            ssTable.append(key, bufOut);
            bf.fill(key);
        }
        ssTable.close(bf);

        // verify
        ssTable = new SSTable(f.getPath() + "-Data.db");
        for ( int i = 100; i < 1000; ++i )
        {
            String key = Integer.toString(i);

            DataInputBuffer bufIn = ssTable.next(key, "Test:C");
            cf = ColumnFamily.serializer().deserialize(bufIn);
            assert cf != null;
            assert cf.name().equals("Test");
            assert cf.getAllColumns().size() == 1;
            assert Arrays.equals(cf.getColumn("C").value(), ("Avinash Lakshman is a good man: " + i).getBytes());
            assert cf.getColumn("C").timestamp() == i;
        }
    }
}
