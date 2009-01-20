package com.facebook.infrastructure.utils;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class BloomFilterTest {
    public BloomFilter bf;
    public BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(0.1);
    static final int ELEMENTS = 10000;

    public BloomFilterTest() {
        spec.bucketsPerElement = 8;
        bf = new BloomFilter(ELEMENTS, spec.bucketsPerElement);
    }

    @BeforeMethod
    public void clear() {
        bf.clear();
    }

    @Test
    public void testOne() {
        bf.add("a");
        assert bf.isPresent("a");
        assert !bf.isPresent("b");
    }

    @Test
    public void testFalsePositivesInt() {
        FilterTest.testFalsePositives(bf, FilterTest.intKeys(), FilterTest.randomKeys2());
    }
    @Test
    public void testFalsePositivesRandom() {
        FilterTest.testFalsePositives(bf, FilterTest.randomKeys(), FilterTest.randomKeys2());
    }
    @Test
    public void testWords() {
        BloomFilter bf2 = new BloomFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTest.spec.bucketsPerElement);
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        FilterTest.testFalsePositives(bf2,
                                      new KeyGenerator.WordGenerator(skipEven, 2),
                                      new KeyGenerator.WordGenerator(1, 2));
    }
    
    @Test
    public void testSerialize() throws IOException {
        FilterTest.testSerialize(bf);
    }

    /* TODO move these into a nightly suite (they take 5-10 minutes each)
    @Test
    // run with -mx1G
    public void testBigInt() {
        int size = 100 * 1000 * 1000;
        bf = new BloomFilter(size, FilterTest.spec.bucketsPerElement);
        FilterTest.testFalsePositives(bf,
                                      new KeyGenerator.IntGenerator(size),
                                      new KeyGenerator.IntGenerator(size, size * 2));
    }

    @Test
    public void testBigRandom() {
        int size = 100 * 1000 * 1000;
        bf = new BloomFilter(size, FilterTest.spec.bucketsPerElement);
        FilterTest.testFalsePositives(bf,
                                      new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                      new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
    }

    @Test
    // run with -server -mx1G
    public void timeit() {
        bf = new BloomFilter(300 * FilterTest.ELEMENTS, FilterTest.spec.bucketsPerElement);
        for (int i = 0; i < 100; i++) {
            FilterTest.testFalsePositives(bf, FilterTest.getRandomKeys(new Random(), 300 * FilterTest.ELEMENTS), FilterTest.getRandomKeys(new Random(), 300 * FilterTest.ELEMENTS));
            bf.clear();
        }
    }
    */
}
