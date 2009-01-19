package com.facebook.infrastructure.utils;

import com.facebook.infrastructure.io.ICompactSerializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public abstract class Filter {
    int hashCount;
    
    // private static JenkinsHash hasher = new JenkinsHash();
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

    /*
     * the approach outlined in
     * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
     * DOES NOT GIVE GOOD RESULTS.  Bit distribution is not uniform
     * and you will see ~10% more false positives than you will
     * by using crypto-quality hash code.
     *
     * (One other alternative that *does* seem to work well is the JenkinsHash from
     * HBase, with initval for hash.i+1 equal to hash.i.)
     */
    static int[] getHashBuckets2(String key, int hashCount, int max) {
        // figure out how many digests are needed
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        int digestsNeeded = (int)Math.round(Math.ceil(hashCount / (md.getDigestLength() / 4.0)));
        byte[] bytes = new byte[md.getDigestLength() * digestsNeeded];

        // compute digests: first, digest the bytes.  specify encoding to avoid getting a byte[] full of '?'.
        byte[] keyBytes;
        try {
            keyBytes = key.getBytes("UTF-16");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        md.update(keyBytes);
        // if we need extra digests, use bytes from the first (and successive, if necessary) to
        // generate extra permutations for additional digests.
        try {
            md.digest(bytes, 0, md.getDigestLength());
            for (int i = 1; i < digestsNeeded; i++) {
                // have to re-init the hash since digest calls engineReset :-|
                md.update(keyBytes);
                // doesn't matter much what we update with as long as it's different per iteration,
                // (I tested several schemes -- the hash algorithm takes care of changing lots of bits)
                // but this should be nice and fast since we already have the bytes sitting there
                md.update(bytes, (i - 1) * 4, 4);
                md.digest(bytes, i * md.getDigestLength(), md.getDigestLength());
            }
        } catch (DigestException e) {
            throw new RuntimeException(e);
        }

        // extract hashes
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int[] hashes = new int[hashCount];
        for (int i = 0; i < hashCount; i++) {
            hashes[i] = Math.abs(bb.getInt(i * 4)) % max;
        }
        return hashes;
    }

    // adapted from hadoop-hbase/src/java/org/onelab/filter/HashFunction.java
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

    // adapted from hadoop-hbase/src/java/org/onelab/filter/HashFunction.java
    static int[] getHashBuckets4(String key, int hashCount, int max) {
        int[] result = new int[hashCount];
        Random r = new Random(key.hashCode());
        for (int i = 0; i < hashCount; i++) {
            result[i] = r.nextInt(max);
        }
        return result;
    }
}