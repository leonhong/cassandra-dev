package com.facebook.infrastructure.db;

import org.testng.annotations.Test;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.ServerTest;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class SystemTableTest extends ServerTest {
    @Test
    public void testMain() throws IOException {
        SystemTable.openSystemTable(SystemTable.cfName_).updateToken( StorageService.hash("503545744:0") );

        List<Range> ranges = new ArrayList<Range>();
        ranges.add( new Range(new BigInteger("1218069462158869448693347920504606362273788442553"), new BigInteger("1092770595533781724218060956188429069")) );
        assert Range.isKeyInRanges(ranges, "304700067:0");
    }
}
