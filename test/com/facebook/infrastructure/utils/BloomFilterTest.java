package com.facebook.infrastructure.utils;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class BloomFilterTest {
    public BloomFilter bf;
    public BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(0.1);
    static final int ELEMENTS = 10000;

    public BloomFilterTest() {
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
        FilterTest.testFalsePositives(bf, FilterTest.intKeys, FilterTest.randomKeys2);
    }
    @Test
    public void testFalsePositivesRandom() {
        FilterTest.testFalsePositives(bf, FilterTest.randomKeys, FilterTest.randomKeys2);
    }

    @Test
    public void testSerialize() throws IOException {
        FilterTest.testSerialize(bf);
    }
}
