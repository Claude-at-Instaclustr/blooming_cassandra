package com.instaclustr.cassandra.bloom.idx.mem;

import static org.junit.Assert.assertEquals;
import static com.instaclustr.cassandra.bloom.idx.mem.BloomTestingHelper.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

public class FlatBloofiTest {

    private static File dir;
    private static final Shape shape = Shape.Factory.fromNP(10, 1.0 / 10);

    public FlatBloofiTest() {
    }

    @BeforeClass
    public static void beforeClass() {
        dir = Files.createTempDir();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        FileUtils.deleteDirectory(dir);
    }

    @After
    public void teardown() throws IOException {
        FileUtils.deleteDirectory(dir);
        dir.mkdirs();
    }

    @Test
    public void addTest() throws IOException {

        try (FlatBloofi bloofi = new FlatBloofi(dir, shape.getNumberOfBits())) {
            assertEquals(0, bloofi.add(asByteBuffer(asHasher("Hello World"), shape)));
            assertEquals(1, bloofi.add(asByteBuffer(asHasher("Good bye cruel World"), shape)));
            assertEquals(2, bloofi.add(asByteBuffer(asHasher("Now is the time"), shape)));
        }

    }

    @Test
    public void countTest() throws IOException {

        try (FlatBloofi bloofi = new FlatBloofi(dir, shape.getNumberOfBits())) {
            assertEquals(0, bloofi.count());
            bloofi.add(asByteBuffer(asHasher("Hello World"), shape));
            assertEquals(1, bloofi.count());
            bloofi.add(asByteBuffer(asHasher("Good bye cruel World"), shape));
            assertEquals(2, bloofi.count());
            bloofi.add(asByteBuffer(asHasher("Now is the time"), shape));
            assertEquals(3, bloofi.count());
        }
    }

    @Test
    public void deleteTest() throws IOException {

        try (FlatBloofi bloofi = new FlatBloofi(dir, shape.getNumberOfBits())) {
            assertEquals(0, bloofi.add(asByteBuffer(asHasher("Hello World"), shape)));
            assertEquals(1, bloofi.add(asByteBuffer(asHasher("Good bye cruel World"), shape)));
            assertEquals(2, bloofi.add(asByteBuffer(asHasher("Now is the time"), shape)));

            bloofi.delete(1);
            assertEquals(2, bloofi.count());

        }
    }

    @Test
    public void searchTest() throws IOException {
        try (FlatBloofi bloofi = new FlatBloofi(dir, shape.getNumberOfBits())) {
            assertEquals(0, bloofi.add(asByteBuffer(asHasher("Hello World"), shape)));
            assertEquals(1, bloofi.add(asByteBuffer(asHasher("Good bye cruel World"), shape)));
            assertEquals(2, bloofi.add(asByteBuffer(asHasher("Now is the time"), shape)));

            ByteBuffer target = asByteBuffer(asHasher("Hello"), shape);
            List<Integer> lst = new ArrayList<Integer>();
            bloofi.search(lst::add, target);
            assertEquals(1, lst.size());

            target = asByteBuffer(asHasher("World"), shape);
            lst.clear();
            bloofi.search(lst::add, target);
            assertEquals(2, lst.size());

            bloofi.delete(1);
            lst.clear();
            bloofi.search(lst::add, target);
            assertEquals(1, lst.size());

        }
    }

    @Test
    public void updateTest() throws IOException {
        try (FlatBloofi bloofi = new FlatBloofi(dir, shape.getNumberOfBits())) {
            assertEquals(0, bloofi.add(asByteBuffer(asHasher("Hello World"), shape)));
            assertEquals(1, bloofi.add(asByteBuffer(asHasher("Good bye cruel World"), shape)));
            assertEquals(2, bloofi.add(asByteBuffer(asHasher("Now is the time"), shape)));

            ByteBuffer target = asByteBuffer(asHasher("Hello"), shape);
            List<Integer> lst = new ArrayList<Integer>();
            bloofi.search(lst::add, target);
            assertEquals(1, lst.size());

            target = asByteBuffer(asHasher("World"), shape);
            lst.clear();
            bloofi.search(lst::add, target);
            assertEquals(2, lst.size());

            bloofi.update(1, asByteBuffer(asHasher("Hello Mr. Chips"), shape));
            lst.clear();
            bloofi.search(lst::add, target);
            assertEquals(1, lst.size());

            target = asByteBuffer(asHasher("Hello"), shape);
            lst.clear();
            bloofi.search(lst::add, target);
            assertEquals(2, lst.size());

        }
    }

}
