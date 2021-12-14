package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.LongBufferBitMap;

public class BloomTableTest {

    private static File dir;
    private File file;
    private static final Shape shape = Shape.Factory.fromNP(10, 1.0 / 10);
    private BusyTable busyTable;

    public BloomTableTest() {
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
    public void setup() throws IOException {
        file = new File(dir, "BloomTable");
        busyTable = new BusyTable(new File(dir, "BusyTable"));
    }

    @After
    public void teardown() throws IOException {
        busyTable.close();
        FileUtils.deleteRecursive(dir);
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
            LongBufferBitMap bitMap = new LongBufferBitMap(bloomTable.getLongBuffer());
            assertTrue("No data was set", bitMap.indices(shape.getNumberOfBits()).hasNext());
            LongBuffer lb = bloomTable.getLongBuffer();

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

}
