package com.facebook.infrastructure.service;

import org.testng.annotations.Test;

import java.math.BigInteger;

public class StorageServiceTest {
    @Test
    public void testHashEmpty() {
        assert StorageService.hash("").equals(BigInteger.ZERO);
    }

    @Test
    public void testHashOrder1() {
        assert StorageService.hash("A").compareTo(StorageService.hash("B")) < 0;
        assert StorageService.hash("A").compareTo(StorageService.hash("AA")) < 0;
        assert StorageService.hash("A").compareTo(StorageService.hash("Z")) < 0;
    }

    @Test
    public void testHashOrder2() {
        assert StorageService.hash("AA").compareTo(StorageService.hash("B")) < 0;
        assert StorageService.hash("AAAAAAAAAAAAAAAAAAAAAAAA").compareTo(StorageService.hash("B")) < 0;
    }
}
