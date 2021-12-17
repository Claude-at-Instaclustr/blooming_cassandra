package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.LongBufferBitMap;

public class BloomTableTest {

    private static File dir;
    private File file;
    private static final Shape shape = Shape.Factory.fromNP(10, 1.0 / 10);
    private BitTable busyTable;

    public BloomTableTest() {
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
    public void setup() throws IOException {
        file = new File(dir, "BloomTable");
        busyTable = new BitTable(new File(dir, "BusyTable"));
    }

    @After
    public void teardown() throws IOException {
        busyTable.close();
        FileUtils.deleteDirectory(dir);
        dir.mkdirs();
    }

    @Test
    public void setBloomTest() throws IOException {
        LongBuffer buffer1 = LongBuffer.allocate(BitMap.numberOfBitMaps(shape.getNumberOfBits()));
        while (buffer1.hasRemaining()) {
            buffer1.put(~0L);
        }
        buffer1.flip();
        try (BloomTable bloomTable = new BloomTable(shape.getNumberOfBits(), file)) {
            bloomTable.setBloomAt(0, buffer1);
            LongBufferBitMap bitMap = new LongBufferBitMap(bloomTable.getBitTable().getLongBuffer());
            assertTrue("No data was set", bitMap.indices(shape.getNumberOfBits()).hasNext());
            LongBuffer lb = bloomTable.getBitTable().getLongBuffer();

            int i = 0;
            while (lb.hasRemaining()) {
                if (i < shape.getNumberOfBits()) {
                    assertEquals("Error at position: " + i, 1, lb.get());
                } else {
                    assertEquals("Error at position: " + i, 0, lb.get());
                }
                i++;
            }

            bloomTable.setBloomAt(2, buffer1);

            i = 0;
            while (lb.hasRemaining()) {
                if (i < shape.getNumberOfBits()) {
                    assertEquals("Error at position: " + i, 5, lb.get());
                } else {
                    assertEquals("Error at position: " + i, 0, lb.get());
                }
                i++;
            }

        }
    }

    @Test
    public void searchTest() throws IOException {
        LongBuffer buffer1 = LongBuffer.allocate(BitMap.numberOfBitMaps(shape.getNumberOfBits()));
        while (buffer1.hasRemaining()) {
            buffer1.put(~0L);
        }
        buffer1.flip();
        LongBuffer buffer2 = LongBuffer.allocate(BitMap.numberOfBitMaps(shape.getNumberOfBits()));
        while (buffer2.hasRemaining()) {
            buffer2.put(0xFFL);
        }
        buffer2.flip();
        try (BloomTable bloomTable = new BloomTable(shape.getNumberOfBits(), file)) {

            bloomTable.setBloomAt(busyTable.newIndex(), buffer1);
            bloomTable.setBloomAt(busyTable.newIndex(), buffer2);
            bloomTable.setBloomAt(busyTable.newIndex(), buffer1);

            LongBuffer searchBuffer = buffer2.duplicate();
            List<Integer> actual = new ArrayList<Integer>();
            bloomTable.search(actual::add, searchBuffer, busyTable);
            assertEquals(3, actual.size());
            assertTrue(actual.contains(0));
            assertTrue(actual.contains(1));
            assertTrue(actual.contains(2));

            searchBuffer = buffer1.duplicate();
            actual.clear();
            bloomTable.search(actual::add, searchBuffer, busyTable);
            assertEquals(2, actual.size());
            assertTrue(actual.contains(0));
            assertTrue(actual.contains(2));

            searchBuffer = buffer1.duplicate();
            busyTable.clear(0);
            actual.clear();
            bloomTable.search(actual::add, searchBuffer, busyTable);
            assertEquals(1, actual.size());
            assertTrue(actual.contains(2));

        }
    }

    @Test
    public void patternTest() throws IOException {
        int numberOfBits = 16;
        ByteBuffer bb1 = ByteBuffer.wrap(new byte[] { 0x01, 0x01 });
        ByteBuffer bb2 = ByteBuffer.wrap(new byte[] { 0x02, 0x02 });
        ByteBuffer bb3 = ByteBuffer.wrap(new byte[] { 0x03, 0x03 });
        ByteBuffer bb4 = ByteBuffer.wrap(new byte[] { 0x04, 0x04 });
        ByteBuffer bb5 = ByteBuffer.wrap(new byte[] { 0x05, 0x05 });
        ByteBuffer bb6 = ByteBuffer.wrap(new byte[] { 0x06, 0x06 });
        ByteBuffer bb7 = ByteBuffer.wrap(new byte[] { 0x07, 0x07 });
        ByteBuffer bb8 = ByteBuffer.wrap(new byte[] { 0x08, 0x08 });
        ByteBuffer bb9 = ByteBuffer.wrap(new byte[] { 0x09, 0x09 });
        ByteBuffer bbA = ByteBuffer.wrap(new byte[] { 0x0A, 0x0A });
        ByteBuffer bbB = ByteBuffer.wrap(new byte[] { 0x0B, 0x0B });
        ByteBuffer bbC = ByteBuffer.wrap(new byte[] { 0x0C, 0x0C });
        ByteBuffer bbD = ByteBuffer.wrap(new byte[] { 0x0D, 0x0D });
        ByteBuffer bbE = ByteBuffer.wrap(new byte[] { 0x0E, 0x0E });
        ByteBuffer bbF = ByteBuffer.wrap(new byte[] { 0x0F, 0x0F });
        try (BloomTable bloomTable = new BloomTable(numberOfBits, file)) {
            for (int i = 1; i < 0x100; i++) {
                LongBuffer lb = LongBuffer.allocate(BitMap.numberOfBitMaps(16));
                lb.put(i);
                lb.flip();
                bloomTable.setBloomAt(1, lb);
                ;
            }

        }
    }
}
