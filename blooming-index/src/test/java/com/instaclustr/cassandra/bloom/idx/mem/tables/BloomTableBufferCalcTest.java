package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

public class BloomTableBufferCalcTest {

    private static File dir;
    private File file;
    private static final Shape shape = Shape.Factory.fromNP(10, 1.0 / 10);
    private BloomTable bloomTable;
    private BloomTable.BufferCalc calc;

    public BloomTableBufferCalcTest() {
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
        bloomTable = new BloomTable(shape.getNumberOfBits(), file);
        calc = bloomTable.new BufferCalc();
    }

    @After
    public void teardown() throws IOException {
        bloomTable.close();
        FileUtils.deleteDirectory(dir);
        dir.mkdirs();
    }

    @Test
    public void getBufferOffsetForIndexTest() throws IOException {
        assertEquals(shape.getNumberOfBits() * Long.BYTES, calc.getLengthInBytes());
        assertEquals(0, calc.getBufferOffsetForIdx(0));
        assertEquals(0, calc.getBufferOffsetForIdx(Long.SIZE - 1));
        assertEquals(calc.getLengthInBytes(), calc.getBufferOffsetForIdx(Long.SIZE));
        assertEquals(2 * calc.getLengthInBytes(), calc.getBufferOffsetForIdx(Long.SIZE * 2));
    }

    @Test
    public void getBufferBitPositionTest() throws IOException {

        /*
         * test block location calculations
         */
        // test first block
        for (int i = 0; i < shape.getNumberOfBits(); i++) {
            int idx = calc.getBufferBitPosition(0, i);
            int upperLimit = (calc.getLengthInBytes() - 1) * Byte.SIZE;
            int pos = (i * (Long.SIZE - 1)) + i;
            assertTrue("Failed at " + i, idx < upperLimit);
            assertEquals("Failed at " + i, pos, idx);

        }

        // test second block
        for (int i = 0; i < shape.getNumberOfBits(); i++) {
            int idx = calc.getBufferBitPosition(Long.SIZE, i);
            int lowerLimit = calc.getLengthInBytes() * Byte.SIZE;
            int upperLimit = (calc.getLengthInBytes() - 1) * 2 * Byte.SIZE;
            int pos = (calc.getLengthInBytes() * Byte.SIZE) + (i * (Long.SIZE - 1)) + i;
            assertTrue("Failed at " + i, idx >= lowerLimit);
            assertTrue("Failed at " + i, idx < upperLimit);
            assertEquals("Failed at " + i, pos, idx);

        }
    }

    @Test
    public void verifyCoverageOfAllBits() throws IOException {

        /**
         * Test index setting operations
         */
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < shape.getNumberOfBits() * Long.SIZE; i++) {
            set.add(i);
        }
        int check = 0;
        for (int i = 0; i < shape.getNumberOfBits(); i++) {
            for (int j = 0; j < Long.SIZE; j++) {
                int idx = calc.getBufferBitPosition(j, i);
                assertEquals(String.format("Failed at (%s %s) %s", j, i, idx), check, idx);
                check++;
                assertTrue(String.format("Failed at (%s %s) %s", j, i, idx), set.remove(idx));
            }
        }
        assertTrue("Set is not empty", set.isEmpty());

        for (int i = shape.getNumberOfBits() * Long.SIZE; i < shape.getNumberOfBits() * Long.SIZE * 2; i++) {
            set.add(i);
        }
        // do not reset check.
        for (int i = 0; i < shape.getNumberOfBits(); i++) {
            for (int j = Long.SIZE; j < Long.SIZE * 2; j++) {
                int idx = calc.getBufferBitPosition(j, i);
                assertEquals(String.format("Failed at (%s %s) %s", j, i, idx), check, idx);
                check++;
                assertTrue(String.format("Failed at (%s %s) %s", j, i, idx), set.remove(idx));
            }
        }
        assertTrue("Set is not empty", set.isEmpty());

    }

    @Test
    public void getBufferSearchPositionTest() throws IOException {
        int check = 0;
        bloomTable.getBitTable().ensureBlock(2);
        long fileSize = bloomTable.getBitTable().getFileSize();
        for (int block = 0; block < 2; block++) {
            for (int bit = 0; bit < shape.getNumberOfBits(); bit++) {
                int position = calc.getBufferSearchPosition(block, bit);
                assertTrue(String.format("Check error at %s (%s, %s)", check, block, bit),
                        check < ((block + 1) * calc.getLengthInBytes()));
                assertEquals(String.format("Position error at %s (%s, %s)", check, block, bit), check, position);
                assertTrue(String.format("Length error at %s (%s, %s) ", position, block, bit), position < fileSize);
                check += Long.BYTES;
            }
        }
    }

    @Test
    public void getNumberOfBlocksTest() throws IOException {
        double ratio = 1.0 * calc.getLengthInBytes() / bloomTable.getBitTable().getBlockSize();
        int factor = calc.getLengthInBytes() / bloomTable.getBitTable().getBlockSize();
        assertEquals(ratio, factor, 0.0001);
        assertEquals(1, calc.getNumberOfBlocks(0));
        assertEquals(1, calc.getNumberOfBlocks(Long.SIZE - 1));
        assertEquals(2, calc.getNumberOfBlocks(Long.SIZE));
        assertEquals(3, calc.getNumberOfBlocks(Long.SIZE * 2));

        for (int i = 0; i < Long.SIZE * 3; i++) {
            assertEquals((i / Long.SIZE) + 1, calc.getNumberOfBlocks(i));
        }

    }

    @Test
    public void verifyBitPositionCalculation() throws IOException {
        teardown();
        int numberOfBits = Long.SIZE + Integer.SIZE;
        file = new File(dir, "BloomTable");
        bloomTable = new BloomTable(numberOfBits, file);
        calc = bloomTable.new BufferCalc();
        Set<Integer> seen = new HashSet<Integer>();
        for (int block = 0; block < 2; block++)
            for (int j = 0; j < 64; j++) {
                for (int i = 0; i < numberOfBits; i++) {
                    int expected = (block * calc.getLengthInBytes() * Byte.SIZE) + j + (i * 64);
                    int actual = calc.getBufferBitPosition((block * 64) + j, i);
                    assertEquals(String.format("(%s %s,%s)", block, j, i), expected, actual);
                    assertTrue("Already saw " + actual, seen.add(actual));

                }
            }
    }

}
