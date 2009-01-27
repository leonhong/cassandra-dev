package com.facebook.infrastructure.service;

import com.facebook.infrastructure.ServerTest;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.thrift.TException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CassandraServerTest extends ServerTest {
    @Test
    public void testCollation() {
        Iterator<String> i1 = Arrays.asList("a", "c", "d").iterator();
        Iterator<String> i2 = Arrays.asList("b", "e", "f", "g").iterator();

        List<String> L = new ArrayList<String>();
        CollectionUtils.addAll(L, IteratorUtils.collatedIterator(CassandraServer.STRING_COMPARATOR, i1, i2));

        List<String> L2 = Arrays.asList(new String[] {"a", "b", "c", "d", "e", "f", "g"});
        assert L.equals(L2);
    }

    @Test
    public void test_get_range() throws IOException, TException {
        CassandraServer server = new CassandraServer();
        server.start();

        // assert CollectionUtils.EMPTY_COLLECTION.equals(server.get_range(DatabaseDescriptor.getTableName(), ""));
        String last = null;
        for (String key : server.get_range(DatabaseDescriptor.getTableName(), "")) {
            if (last != null) {
                assert last.compareTo(key) <= 0;
            }
            last = key;
        }
    }
}
