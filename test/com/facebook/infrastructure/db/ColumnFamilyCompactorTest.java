package com.facebook.infrastructure.db;

import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ColumnFamilyCompactorTest {
    @Test
    public void testGetCompactionBuckets() throws IOException {
        // create files 20 40 60 ... 180
        List<String> small = new ArrayList<String>();
        List<String> med = new ArrayList<String>();
        List<String> big1 = new ArrayList<String>();
        List<String> big2 = new ArrayList<String>();
        List<String> all = new ArrayList<String>();

        String fname;
        fname = createFile(20);
        small.add(fname);
        all.add(fname);
        fname = createFile(40);
        small.add(fname);
        all.add(fname);

        for (int i = 60; i <= 140; i+=20) {
            fname = createFile(i);
            med.add(fname);
            all.add(fname);
        }
        fname = createFile(160);
        big1.add(fname);
        all.add(fname);
        fname = createFile(180);
        big2.add(fname);
        all.add(fname);

        Set<List<String>> buckets = ColumnFamilyCompactor.getCompactionBuckets(all, 50, 150);
        assert buckets.contains(small);
        assert buckets.contains(med);
        assert buckets.contains(big1);
        assert buckets.contains(big2);
    }

    private String createFile(int nBytes) throws IOException {
        File f = File.createTempFile("bucket_test", "");
        FileOutputStream fos = new FileOutputStream(f);
        byte[] bytes = new byte[nBytes];
        fos.write(bytes);
        fos.close();
        return f.getAbsolutePath();
    }
}
