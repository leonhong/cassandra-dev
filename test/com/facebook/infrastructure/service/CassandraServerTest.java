package com.facebook.infrastructure.service;

import com.facebook.infrastructure.ServerTest;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.RangeVerbHandler;
import com.facebook.thrift.TException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class CassandraServerTest extends ServerTest {
    @Test
    public void testCollation() {
        Iterator<String> i1 = Arrays.asList("a", "c", "d").iterator();
        Iterator<String> i2 = Arrays.asList("b", "e", "f", "g").iterator();

        List<String> L = new ArrayList<String>();
        CollectionUtils.addAll(L, IteratorUtils.collatedIterator(RangeVerbHandler.STRING_COMPARATOR, i1, i2));

        List<String> L2 = Arrays.asList(new String[] {"a", "b", "c", "d", "e", "f", "g"});
        assert L.equals(L2);
    }

    /*
    TODO fix resetting server so this works
    @Test
    public void test_get_range_empty() throws IOException, TException {
        CassandraServer server = new CassandraServer();
        server.start();

        assert CollectionUtils.EMPTY_COLLECTION.equals(server.get_range(DatabaseDescriptor.getTableName(), ""));
    }
    */

    /*
    TODO also needs server reset
    @Test
    public void test_get_range() throws IOException, TException {
        CassandraServer server = new CassandraServer();
        server.start();

        // TODO insert some data
        try {
            String last = null;
            for (String key : server.get_range(DatabaseDescriptor.getTableName(), "key1")) {
                if (last != null) {
                    assert last.compareTo(key) < 0;
                }
                last = key;
            }
        } finally {
            server.shutdown();
        }
    }
    */

    @Test
    public void test_get_column() throws IOException, NotFoundException, InvalidRequestException {
        CassandraServer server = new CassandraServer();
        server.start();

        try {
            column_t c1 = new column_t("c1", "0", 0L);
            column_t c2 = new column_t("c2", "0", 0L);
            List<column_t> columns = new ArrayList<column_t>();
            columns.add(c1);
            columns.add(c2);
            Map<String, List<column_t>> cfmap = new HashMap<String, List<column_t>>();
            cfmap.put("Standard1", columns);
            cfmap.put("Standard2", columns);

            batch_mutation_t m = new batch_mutation_t("Table1", "key1", cfmap);
            server.batch_insert_blocking(m);

            column_t column;
            column = server.get_column("Table1", "key1", "Standard1:c2");
            assert column.value.equals("0");

            column = server.get_column("Table1", "key1", "Standard2:c2");
            assert column.value.equals("0");

            ArrayList<column_t> column_ts = server.get_slice_strong("Table1", "key1", "Standard1", -1, -1);
            assert column_ts.size() == 2;
        } finally {
            server.shutdown();
        }
    }
}
