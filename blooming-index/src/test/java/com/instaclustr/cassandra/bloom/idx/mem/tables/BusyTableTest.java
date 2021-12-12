package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static com.instaclustr.cassandra.bloom.idx.mem.tables.AbstractTableTestHelpers.assertNoLocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntConsumer;

import org.apache.cassandra.io.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BusyTable;

public class BusyTableTest {

    private static File dir;
    private File file;

    public BusyTableTest() {
        // TODO Auto-generated constructor stub
    }

    @BeforeClass
    public static void beforeClass() {
        dir = Files.createTempDir();
    }

    public static void afterClass() {
        FileUtils.deleteRecursive(dir);
    }

    @Before
    public void setup() {
        file = new File(dir, "busy");
    }

    @After
    public void teardown() {
        FileUtils.delete(file);
    }

    @Test
    public void newIndexTest() throws IOException {
        try (BusyTable busy = new BusyTable(file)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(i, busy.newIndex());
            }
            FileInputStream fis = new FileInputStream(file);
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0x03, fis.read());
            assertEquals(0xFF, fis.read());
            assertNoLocks(busy);
        }
    }

    @Test
    public void clearTest() throws IOException {
        try (BusyTable busy = new BusyTable(file)) {

            for (int i = 0; i < 10; i++) {
                assertEquals(i, busy.newIndex());
            }

            busy.clear(5);

            FileInputStream fis = new FileInputStream(file);
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0, fis.read());
            assertEquals(0x03, fis.read());
            assertEquals(0xDF, fis.read());
            assertNoLocks(busy);
        }
    }

    @Test
    public void reuseTest() throws IOException {
        try (BusyTable busy = new BusyTable(file)) {
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
        try (BusyTable busy = new BusyTable(file)) {
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
        try (BusyTable busy = new BusyTable(file)) {
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
        try (BusyTable busy = new BusyTable(file)) {
            assertFalse(busy.isSet(5));
            assertNoLocks(busy);
        }
    }


}
