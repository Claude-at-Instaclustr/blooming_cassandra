package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTableTestHelpers.assertNoLocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

public class BitTableTest {

    private static File dir;
    private File file;

    public BitTableTest() {
        // TODO Auto-generated constructor stub
    }

    @BeforeClass
    public static void beforeClass() {
        dir = Files.createTempDir();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        FileUtils.deleteDirectory(dir);
    }

    @Before
    public void setup() {
        file = new File(dir, "busy");
    }

    @After
    public void teardown() throws IOException {
        FileUtils.delete(file);
    }

    @Test
    public void newIndexTest() throws IOException {
        try (BitTable busy = new BitTable(file)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(i, busy.newIndex());
            }
            try (FileInputStream fis = new FileInputStream(file)) {
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0x03, fis.read());
                assertEquals(0xFF, fis.read());
            }
            assertNoLocks(busy);
        }
    }

    @Test
    public void clearTest() throws IOException {
        try (BitTable busy = new BitTable(file)) {

            for (int i = 0; i < 10; i++) {
                assertEquals(i, busy.newIndex());
            }

            busy.clear(5);

            try (FileInputStream fis = new FileInputStream(file)) {
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0, fis.read());
                assertEquals(0x03, fis.read());
                assertEquals(0xDF, fis.read());
            }
            assertNoLocks(busy);
        }
    }

    @Test
    public void reuseTest() throws IOException {
        try (BitTable busy = new BitTable(file)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(i, busy.newIndex());
            }

            busy.clear(5);

            assertEquals(5, busy.newIndex());
            assertNoLocks(busy);
        }
    }

    @Test
    public void cardinalityTest() throws IOException {
        try (BitTable busy = new BitTable(file)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(i, busy.newIndex());
            }

            assertEquals(10, busy.cardinality());
            busy.clear(5);
            assertEquals(9, busy.cardinality());
            assertNoLocks(busy);
        }
    }

    @Test
    public void isSetTest() throws IOException {
        try (BitTable busy = new BitTable(file)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(i, busy.newIndex());
            }

            assertTrue(busy.isSet(5));
            busy.clear(5);
            assertFalse(busy.isSet(5));
            assertNoLocks(busy);
        }
    }

    @Test
    public void isSetNotWrittenTest() throws IOException {
        try (BitTable busy = new BitTable(file)) {
            assertFalse(busy.isSet(5));
            assertNoLocks(busy);
        }
    }

    @Test
    public void boundaryCrossingTest() throws IOException {
        try (BitTable busy = new BitTable(file)) {
            for (int i = 0; i < Long.SIZE; i++) {
                assertEquals(i, busy.newIndex());
                assertEquals(Long.BYTES, busy.getFileSize());
            }
            assertEquals(Long.SIZE, busy.newIndex());
            assertEquals(Long.BYTES * 2, busy.getFileSize());
            assertEquals(Long.SIZE + 1, busy.newIndex());
            assertEquals(Long.BYTES * 2, busy.getFileSize());
        }

    }

}
