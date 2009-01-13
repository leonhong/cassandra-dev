package com.facebook.infrastructure.utils;

import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.DataOutputBuffer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class FilterTest {
    public void testManyHashes(Collection<String> keys) {
        int MAX_HASH_COUNT = 128;
        Set<Integer> hashes = new HashSet<Integer>();
        int collisions = 0;
        for (String key : keys) {
            hashes.clear();
            for (int hashIndex : Filter.getHashBuckets(key, MAX_HASH_COUNT, 1024*1024)) {
                hashes.add(hashIndex);
            }
            collisions += (MAX_HASH_COUNT - hashes.size());
        }
        System.out.println("Collisions: " + collisions);
        assert collisions <= 100;
    }

    @Test
    public void testManyInt() {
        testManyHashes(intKeys);
    }
    @Test
    public void testManyRandom() {
        testManyHashes(randomKeys);
    }

    // used by filter subclass tests
    
    public static final BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(0.1);
    static final int ELEMENTS = 10000;

    static final Collection<String> intKeys = Collections.unmodifiableCollection(new ArrayList<String>() {{
        for (int i = 0; i < ELEMENTS; i++) {
            add(Integer.toString(i));
        }
    }});

    private static String randomKey(Random r) {
        StringBuffer buffer = new StringBuffer();
        for (int j = 0; j < 16; j++) {
            buffer.append((char)r.nextInt());
        }
        return buffer.toString();
    }

    static final Collection<String> randomKeys = Collections.unmodifiableCollection(new HashSet<String>() {{
        Random r = new Random(314159);
        while (size() < ELEMENTS) {
            add(randomKey(r));
        }
        assert size() == ELEMENTS;
    }});
    static final Collection<String> randomKeys2 = Collections.unmodifiableCollection(new HashSet<String>() {{
        Random r = new Random(271828);
        while (size() < ELEMENTS) {
            String key = randomKey(r);
            if (!randomKeys.contains(key)) {
                add(randomKey(r));
            }
        }
    }});


    public static void testFalsePositives(Filter f, Collection<String> keys, Collection<String> otherkeys) {
        assert keys.size() == ELEMENTS;
        assert otherkeys.size() == ELEMENTS;

        for (String key : keys) {
            f.add(key);
        }
        for (String key : keys) {
            assert f.isPresent(key);
        }

        int fp = 0;
        for (String key : otherkeys) {
            if (f.isPresent(key)) {
                fp++;
            }
        }
        System.out.println("False positives: " + fp);
        assert fp < 1.03 * ELEMENTS * BloomCalculations.getFailureRate(spec.bucketsPerElement);
    }

    public static Filter testSerialize(Filter f) throws IOException {
        f.add("a");
        DataOutputBuffer out = new DataOutputBuffer();
        f.tserializer().serialize(f, out);

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        Filter f2 = (Filter)f.tserializer().deserialize(in);

        assert f2.isPresent("a");
        assert !f2.isPresent("b");
        return f2;
    }

}
