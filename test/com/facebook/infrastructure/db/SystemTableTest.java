package com.facebook.infrastructure.db;

import com.facebook.infrastructure.ServerTest;
import com.facebook.infrastructure.service.StorageService;
import org.testng.annotations.Test;

import java.io.IOException;

public class SystemTableTest extends ServerTest {
    @Test
    public void testMain() throws IOException {
        SystemTable.openSystemTable(SystemTable.cfName_).updateToken( StorageService.hash("503545744:0") );
    }
}
