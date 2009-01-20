package com.facebook.infrastructure.utils;

import com.facebook.infrastructure.io.ICompactSerializer;

import java.io.UnsupportedEncodingException;

public abstract class Filter {
    int hashCount;
    
    private static MurmurHash hasher = new MurmurHash();

    int getHashCount()
    {
        return hashCount;
    }

    public int[] getHashBuckets(String key) {
        return Filter.getHashBuckets(key, hashCount, buckets());
    }

    abstract int buckets();
    public abstract void add(String key);
    public abstract boolean isPresent(String key);

    // for testing
    abstract ICompactSerializer tserializer();
    abstract int emptyBuckets();

    // adapted from hadoop-hbase/src/java/org/onelab/filter/HashFunction.java.
    // this is faster than a sha-based approach and provides as-good collision
    // resistance.  http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
    // does not add anything.
    static int[] getHashBuckets(String key, int hashCount, int max) {
        byte[] b;
        try {
            b = key.getBytes("UTF-16");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        int[] result = new int[hashCount];
        for (int i = 0, initval = 0; i < hashCount; i++) {
            initval = hasher.hash(b, b.length, initval);
            result[i] = Math.abs(initval) % max;
        }
        return result;
    }
}