package com.facebook.infrastructure.db;

import org.testng.annotations.Test;
import com.facebook.infrastructure.ServerTest;

public class DBManagerTest extends ServerTest {
    @Test
    public void testMain() throws Throwable {
        // TODO clean up old detritus
        DBManager.instance().start();
    }
}
