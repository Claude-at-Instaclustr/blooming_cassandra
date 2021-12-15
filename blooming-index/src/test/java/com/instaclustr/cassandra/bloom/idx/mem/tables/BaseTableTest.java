package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.Func;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.RangeLock;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.SyncException;

public class BaseTableTest {

    private static File dir;
    private File file;

    public BaseTableTest() {
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
        file = new File(dir, "BaseTable");
    }

    @After
    public void teardown() throws IOException {
        FileUtils.delete(file);
    }

    @Test
    public void ensureBlockTest() throws IOException {
        try (BaseTable table = new BaseTable(file, 5)) {
            assertEquals(0L, table.getFileSize());
            // ensure block 0 exists
            table.ensureBlock(0);
            assertEquals(5L, table.getFileSize());

            // create a block after a gap
            table.ensureBlock(9);
            assertEquals(50L, table.getFileSize());
        }
    }

    @Test
    public void execQuietlyTest() throws IOException {
        try (BaseTable table = new BaseTable(file, 5)) {
            try {
                table.execQuietly(() -> {
                    throw new Exception("execQuietlyTest This Exception is expected");
                });
            } catch (Exception e) {
                fail("Should not have thrown Exception");
            }
            try {
                table.execQuietly(() -> {
                    throw new RuntimeException("execQuietlyTest This Runtime Exception is expected");
                });
            } catch (RuntimeException e) {
                fail("Should not have thrown RuntimeException");
            }

            final class X implements Func {
                int value = 0;

                @Override
                public void call() throws Exception {
                    value += 5;
                }

            }
            X x = new X();
            table.execQuietly(x);
            assertEquals(5, x.value);
        }
    }

    @Test
    public void extendBufferTest() throws IOException {
        try (BaseTable table = new BaseTable(file, 5)) {
            assertEquals(0L, table.getFileSize());
            table.extendBuffer();
            assertEquals(5L, table.getFileSize());
            table.extendBuffer(9);
            assertEquals(50L, table.getFileSize());
            assertThrows(IllegalArgumentException.class, () -> table.extendBuffer(-1));

        }
    }

    @Test
    public void extendBufferAfteFactionalAddWorks() throws IOException {
        try (BaseTable table = new BaseTable(file, 5)) {

            // fractional blocks do not increment the block count
            table.extendBytes(2);
            assertFalse(table.hasBlock(0));
            assertEquals(2L, table.getFileSize());
            // extending the buffer by a block
            table.extendBuffer();
            assertTrue(table.hasBlock(0));
            // verify additional bytes still exist.
            assertEquals(7L, table.getFileSize());

        }
    }

    @Test
    public void extendBytesTest() throws IOException {
        try (BaseTable table = new BaseTable(file, 5)) {
            assertEquals(0L, table.getFileSize());
            table.extendBytes(4);
            assertEquals(4L, table.getFileSize());
            table.extendBytes(9);
            assertEquals(13L, table.getFileSize());
            assertThrows(IllegalArgumentException.class, () -> table.extendBytes(-1));
        }
    }

    @Test
    public void getLockTest() throws IOException {

        try (BaseTable table = new BaseTable(file, 5)) {
            try (RangeLock rangeLock = table.getLock(0, 10, 1)) {
                assertEquals(0, rangeLock.getStart());
                assertTrue(rangeLock.hasLock());
                assertEquals(2, rangeLock.lockCount());
            }

            try (RangeLock rangeLock = table.getLock(1000, 10, 1)) {
                assertEquals(1000, rangeLock.getStart());
                assertTrue(rangeLock.hasLock());
                assertEquals(2, rangeLock.lockCount());
            }

            try (RangeLock rangeLock = table.getLock(0, 10, 0)) {
                assertEquals(0, rangeLock.getStart());
                assertTrue(rangeLock.hasLock());
                assertEquals(2, rangeLock.lockCount());
            }

            assertThrows(IllegalArgumentException.class, () -> table.getLock(-1, 10, 1));
            assertThrows(IllegalArgumentException.class, () -> table.getLock(0, -1, 1));
            assertThrows(IllegalArgumentException.class, () -> table.getLock(0, 0, 1));
            assertThrows(IllegalArgumentException.class, () -> table.getLock(0, 10, -1));
        }
    }

    @Test
    public void getLockCountTest() throws IOException {

        try (BaseTable table = new BaseTable(file, 5)) {
            assertEquals(0, table.getLockCount());

            try (RangeLock rangeLock = table.getLock(0, 5, 1)) {
                assertEquals(1, table.getLockCount());
                try (RangeLock rangeLock2 = table.getLock(5, 5, 1)) {
                    assertEquals(2, table.getLockCount());
                }
                assertEquals(2, table.getLockCount());
            }
            assertEquals(2, table.getLockCount());
        }

    }

    @Test
    public void getLockedBlocksTest() throws IOException {
        List<Integer> lst = new ArrayList<Integer>();
        try (BaseTable table = new BaseTable(file, 5)) {
            table.getLockedBlocks(lst::add);
            assertEquals(0, lst.size());
            lst.clear();
            try (RangeLock rangeLock = table.getLock(0, 5, 1)) {
                table.getLockedBlocks(lst::add);
                assertEquals(1, lst.size());
                lst.clear();
                try (RangeLock rangeLock2 = table.getLock(5, 5, 1)) {
                    table.getLockedBlocks(lst::add);
                    assertEquals(2, lst.size());
                    lst.clear();
                }
                table.getLockedBlocks(lst::add);
                assertEquals(1, lst.size());
                lst.clear();
            }
            table.getLockedBlocks(lst::add);
            assertEquals(0, lst.size());
            lst.clear();

        }
    }

    @Test
    public void hasBlockTest() throws IOException {
        try (BaseTable table = new BaseTable(file, 5)) {
            assertFalse(table.hasBlock(0));
            // verify looking didn't change things
            assertFalse(table.hasBlock(0));

            table.ensureBlock(0);
            assertTrue(table.hasBlock(0));
            assertFalse(table.hasBlock(1));

            // show fractional blocks do not increment the block count
            table.extendBytes(2);
            assertFalse(table.hasBlock(1));
        }
    }

    @Test
    public void syncTest() throws IOException {
        List<Integer> lst = new ArrayList<Integer>();

        try (BaseTable table = new BaseTable(file, 5)) {
            // no locked buffers
            assertEquals(0, table.getLockCount());

            table.sync(() -> {
                table.getLockedBlocks(lst::add);
                return null;
            }, 0, 1, 0);

            // we created one
            assertEquals(1, table.getLockCount());

            // it was active during the sync execution.
            assertEquals(1, lst.size());

            // it is not active now.
            lst.clear();
            table.getLockedBlocks(lst::add);

            // show error propigation
            assertThrows(SyncException.class, () -> table.sync(() -> {
                throw new IllegalArgumentException();
            }, 0, 1, 0));
        }
    }

}
