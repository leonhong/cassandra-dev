package com.facebook.infrastructure.db;

import com.facebook.infrastructure.ServerTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Random;

public class ColumnFamilyStoreTest extends ServerTest {
    @Test
    public void testMain() throws IOException, ColumnFamilyNotDefinedException {
        Table table = Table.open("Table1");
        Random random = new Random();
        byte[] bytes1 = new byte[1024];
        byte[] bytes2 = new byte[1024];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);

        for (int i = 800; i < 1000; ++i)
        {
            String key = Integer.toString(i);
            RowMutation rm = new RowMutation("Table1", key);
            for ( int j = 0; j < 8; ++j )
            {
                for ( int k = 0; k < 8; ++k )
                {
                    byte[] bytes = (j + k) % 2 == 0 ? bytes1 : bytes2;
                    rm.add("Super1:" + "SuperColumn-" + j + ":Column-" + k, bytes, k);
                    rm.add("Standard1:" + "Column-" + k, bytes, k);
                }
            }
            rm.apply();
        }

        for ( int i = 800; i < 1000; ++i )
        {
            String key = Integer.toString(i);
            // TODO actually test results
            ColumnFamily cf = table.get(key, "Super1:SuperColumn-1");
            assert cf != null;
            Collection<IColumn> superColumns = cf.getAllColumns();
            for ( IColumn superColumn : superColumns )
            {
                Collection<IColumn> subColumns = superColumn.getSubColumns();
                for ( IColumn subColumn : subColumns )
                {
                    //System.out.println(subColumn);
                }
            }
        }
    }
}
