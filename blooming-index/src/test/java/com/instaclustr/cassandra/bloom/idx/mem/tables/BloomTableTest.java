package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

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
    public void writeTest() throws IOException {
        int numberOfBits = Long.SIZE + Integer.SIZE;
        ByteBuffer bb = ByteBuffer.allocate(2 * Long.BYTES);
        long t = ~0L;
        long t2 = t & 0xFFFFFFFFL;
        bb.putLong(t).putLong(t2).flip();

        try (BloomTable bloomTable = new BloomTable(numberOfBits, file)) {
            bloomTable.setBloomAt(1, bb.asLongBuffer());
            assertEquals(numberOfBits * Long.BYTES, bloomTable.getBitTable().getFileSize());
            try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
                for (int i = 0; i < numberOfBits; i++) {
                    long fl = dis.readLong();
                    assertEquals("error at long " + i, 2L, fl);
                }
            }
            try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
                long[] expected = { t, t2 };
                int[] jLimit = { Long.SIZE, Integer.SIZE };
                for (int i = 0; i < 2; i++) {
                    long l = 0;
                    for (int j = 0; j < jLimit[i]; j++) {
                        long fl = dis.readLong();
                        if (fl > 0) {
                            l |= BitMap.getLongBit(j);
                        }
                    }
                    assertEquals(expected[i], l);
                }
            }
            IndexProducer producer = bloomTable.getBloomAt(1);
            BitMapProducer bmp = BitMapProducer.fromIndexProducer(producer, numberOfBits);
            bb.position(0);
            assertTrue(bmp.forEachBitMap((bitMap) -> {
                return bb.getLong() == bitMap;
            }));
        }
    }

    @Test
    public void rountTripTest() throws IOException {
        int numberOfBits = 302;
        ByteBuffer bb = ByteBuffer.allocate(5 * Long.BYTES);
        bb.putLong(0x2600201000084008L).putLong(0x9008804002010200L).putLong(0x4220030080048000L)
                .putLong(0x0c00881008204020L).putLong(0x0000006208010800L).flip();

        try (BloomTable bloomTable = new BloomTable(numberOfBits, file)) {
            bloomTable.setBloomAt(1, bb.asLongBuffer());
            IndexProducer producer = bloomTable.getBloomAt(1);
            BitMapProducer bmp = BitMapProducer.fromIndexProducer(producer, numberOfBits);
            bb.position(0);
            assertTrue(bmp.forEachBitMap((bitMap) -> {
                return bb.getLong() == bitMap;
            }));
        }
    }
}
