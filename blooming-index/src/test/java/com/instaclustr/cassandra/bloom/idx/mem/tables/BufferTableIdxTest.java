package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTableTestHelpers.assertNoLocks;
import java.io.File;
import java.io.IOException;
import org.apache.cassandra.io.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BufferTableIdx.IdxEntry;

public class BufferTableIdxTest {

    private static File dir;
    private File file;

    public BufferTableIdxTest() {
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
        file = new File(dir, "BufferTableIdx");
    }

    @After
    public void teardown() {
        FileUtils.delete(file);
    }

    @Test
    public void getTest() throws IOException {
        try (BufferTableIdx idxTable = new BufferTableIdx(file)) {
            IdxEntry entry0 = idxTable.get(0);
            IdxEntry entry1 = idxTable.get(1);
            assertNoLocks(idxTable);
        }
    }

    @Test
    public void setValuesTest() throws IOException {
        try (BufferTableIdx idxTable = new BufferTableIdx(file)) {
            IdxEntry entry0 = idxTable.get(5);
            IdxEntry entry1 = idxTable.get(5);

            entry0.setOffset(1);
            entry0.setLen(2);
            entry0.setAlloc(3);

            assertEquals(1, entry1.getOffset());
            assertEquals(2, entry1.getLen());
            assertEquals(3, entry1.getAlloc());
            assertNoLocks(idxTable);
        }
    }

    @Test
    public void setFlagsTest() throws IOException {

        // deleted, initialized, invalid, available
        boolean[][] states = { { false, false, false, false }, { false, true, false, false },
            { true, false, true, false }, { true, true, false, true }

        };
        try (BufferTableIdx idxTable = new BufferTableIdx(file)) {
            IdxEntry entry0 = idxTable.get(1);
            IdxEntry entry1 = idxTable.get(1);

            for (int i = 0; i < 4; i++) {
                entry0.setDeleted(states[i][0]);
                entry0.setInitialized(states[i][1]);
                assertEquals(String.format("State %s deleted", i), states[i][0], entry1.isDeleted());
                assertEquals(String.format("State %s initialized", i), states[i][1], entry1.isInitialized());
                assertEquals(String.format("State %s invalid", i), states[i][2], entry1.isInvalid());
                assertEquals(String.format("State %s available", i), states[i][3], entry1.isAvailable());
            }
            assertNoLocks(idxTable);
        }
    }

    @Test
    public void searchTest() throws IOException {
        try (BufferTableIdx idxTable = new BufferTableIdx(file)) {
            assertNull(idxTable.search(500));
            IdxEntry entry0 = idxTable.addBlock(5, 500);
            int idx = entry0.getBlock();

            entry0.setDeleted(true);
            entry0.setInitialized(true);
            IdxEntry entry1 = idxTable.search(500);
            assertNotNull(entry1);
            assertEquals(idx, entry1.getBlock());
            assertFalse(entry1.isDeleted());
            assertFalse(entry1.isInitialized());

            entry0.setDeleted(true);
            entry0.setInitialized(false);
            entry1 = idxTable.search(500);
            assertNull(entry1);

            entry0.setDeleted(false);
            entry0.setInitialized(false);
            entry1 = idxTable.search(500);
            assertNull(entry1);

            entry0.setDeleted(false);
            entry0.setInitialized(true);
            entry1 = idxTable.search(500);
            assertNull(entry1);
            assertNoLocks(idxTable);
        }
    }

    @Test
    public void searchTest_MultipleEntries() throws IOException {
        try (BufferTableIdx idxTable = new BufferTableIdx(file)) {
            assertNull(idxTable.search(500));
            IdxEntry entry0 = idxTable.addBlock(5, 500);
            entry0.setDeleted(false);

            IdxEntry entry1 = idxTable.addBlock(4, 400);
            entry1.setDeleted(true);

            IdxEntry entry2 = idxTable.addBlock(7, 700);
            entry2.setDeleted(true);

            IdxEntry entry3 = idxTable.search(500);
            assertNotNull(entry3);
            assertEquals(entry2.getBlock(), entry3.getBlock());
            assertFalse(entry3.isDeleted());
            assertFalse(entry3.isInitialized());
            assertEquals(700, entry3.getAlloc());
            assertEquals(700, entry3.getLen());
            assertEquals(7, entry3.getOffset());
            assertNoLocks(idxTable);
        }
    }

    @Test
    public void addBlockTest() throws IOException {
        try (BufferTableIdx idxTable = new BufferTableIdx(file)) {
            assertEquals(0, idxTable.getFileSize());
            IdxEntry entry0 = idxTable.addBlock(5, 500);
            assertEquals(0, entry0.getBlock());
            assertEquals(5, entry0.getOffset());
            assertEquals(500, entry0.getLen());
            assertEquals(500, entry0.getAlloc());
            assertFalse(entry0.isAvailable());
            assertFalse(entry0.isDeleted());
            assertTrue(entry0.isInitialized());
            assertFalse(entry0.isInvalid());

            // test adding a second block
            entry0 = idxTable.addBlock(4, 400);
            assertEquals(1, entry0.getBlock());
            assertEquals(4, entry0.getOffset());
            assertEquals(400, entry0.getLen());
            assertEquals(400, entry0.getAlloc());
            assertFalse(entry0.isAvailable());
            assertFalse(entry0.isDeleted());
            assertTrue(entry0.isInitialized());
            assertFalse(entry0.isInvalid());
            assertEquals(2 * idxTable.getBlockSize(), idxTable.getFileSize());
            assertNoLocks(idxTable);
        }
    }

    @Test
    public void deleteBlockTest() throws IOException {
        try (BufferTableIdx idxTable = new BufferTableIdx(file)) {
            assertEquals(0, idxTable.getFileSize());
            IdxEntry entry0 = idxTable.addBlock(1, 500); // block 0
            IdxEntry entry1 = idxTable.addBlock(2, 500); // block 1
            IdxEntry entry2 = idxTable.addBlock(3, 500); // block 2

            idxTable.deleteBlock(entry1.getBlock());

            // verify table
            assertFalse(entry0.isDeleted());
            assertTrue(entry1.isDeleted());
            assertFalse(entry2.isDeleted());

            // verify scanner

            BufferTableIdx.Scanner scanner = idxTable.new Scanner(idxTable.getBuffer(), 0);
            assertTrue(scanner.hasNext());
            assertEquals(entry1.getBlock(), scanner.next().getBlock());
            assertFalse(scanner.hasNext());
            assertNoLocks(idxTable);
        }

    }
}
