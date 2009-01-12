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
import com.facebook.infrastructure.ServerTest;

public class SSTableTest extends ServerTest {
    // TODO test table/CFs that are actually in the storage conf.  this breaks the current test when Name indexing is on, meaning this is a crappy test.
    @Test
    public void testOne() throws IOException {
        File f = File.createTempFile("sstable", "");
        ColumnFamily cf;
        SSTable ssTable;

        // write test data
        ssTable = new SSTable(f.getParent(), f.getName());
        BloomFilter bf = new BloomFilter(1000, 8);
        Random random = new Random();
        byte[] bytes = new byte[1024*1024];
        random.nextBytes(bytes);

        String key = Integer.toString(1);
        cf = new ColumnFamily("Test", "Standard");
        cf.createColumn("C", bytes, 1);
        DataOutputBuffer bufOut = new DataOutputBuffer();
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
