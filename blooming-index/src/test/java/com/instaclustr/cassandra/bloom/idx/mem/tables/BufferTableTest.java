package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTableTestHelpers.assertNoLocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BufferTableIdx.IdxEntry;

public class BufferTableTest {

    private static File dir;
    private File file;

    public BufferTableTest() {
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
        file = new File(dir, "BufferTable");
    }

    @After
    public void teardown() throws IOException {
        FileUtils.deleteDirectory(dir);
        dir.mkdirs();
    }

    private void checkLocks(BufferTable table) {
        assertNoLocks(table);
        assertNoLocks(table.idxTable);
        assertNoLocks(table.keyTableIdx);

    }

    @Test
    public void setTest() throws IOException {
        try (BufferTable table = new BufferTable(file, 1024)) {
            table.set(1, ByteBuffer.wrap("Hello World".getBytes()));
            table.set(2, ByteBuffer.wrap("Good bye Cruel World".getBytes()));
            table.set(3, ByteBuffer
                    .wrap("Now is the time for all good people to come to the aid of their planet".getBytes()));
            checkLocks(table);
        }
    }

    @Test
    public void testDelete() throws IOException {
        try (BufferTable table = new BufferTable(file, 1024)) {
            table.set(1, ByteBuffer.wrap("Hello World".getBytes()));
            table.set(2, ByteBuffer.wrap("Good bye Cruel World".getBytes()));
            table.set(3, ByteBuffer
                    .wrap("Now is the time for all good people to come to the aid of their planet".getBytes()));

            table.delete(2);

            // external idx 2 points to internal key idx 1.
            IdxEntry idxEntry = table.keyTableIdx.get(1);
            assertTrue(idxEntry.isDeleted());
            assertTrue(idxEntry.isAvailable());
            checkLocks(table);
        }

    }

    @Test
    public void testGet() throws IOException {
        try (BufferTable table = new BufferTable(file, 1024)) {
            table.set(1, ByteBuffer.wrap("Hello World".getBytes()));
            table.set(2, ByteBuffer.wrap("Good bye Cruel World".getBytes()));
            table.set(3, ByteBuffer
                    .wrap("Now is the time for all good people to come to the aid of their planet".getBytes()));

            table.delete(2);

            ByteBuffer result = table.get(1);
            assertEquals(ByteBuffer.wrap("Hello World".getBytes()), result);

            result = table.get(2);
            assertNull(result);

            result = table.get(3);
            assertEquals(
                    ByteBuffer
                    .wrap("Now is the time for all good people to come to the aid of their planet".getBytes()),
                    result);

            assertNull("before first should fail", table.get(0));
            assertNull("after last should fail", table.get(500));

        }

    }

    @Test
    public void testReuseDeleted() throws IOException {
        try (BufferTable table = new BufferTable(file, 1024)) {
            table.set(1, ByteBuffer.wrap("Hello World".getBytes()));
            table.set(2, ByteBuffer.wrap("Good bye Cruel World".getBytes()));
            table.set(3, ByteBuffer
                    .wrap("Now is the time for all good people to come to the aid of their planet".getBytes()));

            table.delete(2);

            // external idx 2 points to internal key idx 1.
            IdxEntry idxEntry = table.keyTableIdx.get(1);
            assertTrue(idxEntry.isDeleted());
            assertTrue(idxEntry.isAvailable());

            table.set(2, ByteBuffer.wrap("When in the course".getBytes()));
            assertFalse(idxEntry.isDeleted());
            assertFalse(idxEntry.isAvailable());
            assertEquals(18, idxEntry.getLen());
            assertEquals(20, idxEntry.getAlloc());

            ByteBuffer result = table.get(1);
            assertEquals(ByteBuffer.wrap("Hello World".getBytes()), result);

            result = table.get(2);
            assertEquals(ByteBuffer.wrap("When in the course".getBytes()), result);

            result = table.get(3);
            assertEquals(
                    ByteBuffer
                    .wrap("Now is the time for all good people to come to the aid of their planet".getBytes()),
                    result);

            checkLocks(table);
        }

    }

}
