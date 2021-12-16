package com.instaclustr.cassandra.bloom.idx.mem;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable;


/**
 * These tests read the Dueling Machine text and then verify various aspects of the system.
 *
 */
public class TheDuelingMachineTests {

    static final Shape shape = Shape.Factory.fromNP( 12, 1.0/1000 );
    static FlatBloofi flatBloofi;
    static File dir;
    static int maxIdx;
    static List<String> lines = new ArrayList<String>();
    static final int LINE_COUNT = 2574;

    public TheDuelingMachineTests() {

    }

    @BeforeClass
    public static void setupClass() throws IOException {
        dir = Files.createTempDir();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        if (flatBloofi != null) {
            BaseTable.closeQuietly(flatBloofi );
        }
        FileUtils.deleteDirectory( dir );
    }

    @Before
    public void setup() throws IOException {
        flatBloofi = new FlatBloofi(dir, shape.getNumberOfBits());
    }

    @After
    public void tearDown() throws IOException {
        if (flatBloofi != null) {
            BaseTable.closeQuietly(flatBloofi );
            flatBloofi = null;
        }
        FileUtils.deleteDirectory( dir );
        dir.mkdirs();
        lines.clear();
    }

    private static ByteBuffer asByteBuffer( String string ) {
        return BloomTestingHelper.asByteBuffer( BloomTestingHelper.asHasher(string), shape);
    }

    private static void loadBook() throws IOException {
        lines.clear();
        URL url = TheDuelingMachineTests.class.getResource("/TheDuelingMachine.txt");
        BufferedReader reader = new BufferedReader( new InputStreamReader( url.openStream()));
        maxIdx = -1;
        while (reader.ready()) {
            String line = reader.readLine().trim();
            if (line.length() > 0) {
                lines.add( line );
                int idx = flatBloofi.add( asByteBuffer( line  ));
                maxIdx = maxIdx>idx?maxIdx:idx;
            }
        }
        System.out.println(String.format( "Read %s lines as %s indexes", lines.size(), maxIdx));
    }

    @Test
    public void findLineTest() throws IOException {
        loadBook();
        List<Integer> lst = new ArrayList<Integer>();
        flatBloofi.search( lst::add, asByteBuffer( "him. Those were the terms of the duel. He fingered the stubby"));
        assertEquals( 1, lst.size() );
        assertEquals( "him. Those were the terms of the duel. He fingered the stubby", lines.get( lst.get(0)));
        lst.clear();
        flatBloofi.search( lst::add, asByteBuffer( "Dulaq"));
        int matches=0;
        System.out.println("Looking for 'Dulaq', got:");
        for (Integer idx : lst ) {
            System.out.println( lines.get( idx ));
            if (lines.get(idx).contains( "Dulaq")) {
                matches++;
            }
        }
        System.out.println(String.format( "\nThat is %s/%s matches.", matches, lst.size() ));
        assertEquals( 7, matches  );
    }

    @Test
    public void busyTableCheck() throws FileNotFoundException, IOException {
        loadBook();
        File file = new File( dir, "BusyTable" );
        double expected_longs =  LINE_COUNT *1.0 / Long.SIZE;
        int expectedFullBytes = (int) expected_longs * Long.BYTES;
        long expectedBytes = (long) Math.ceil( expected_longs * Long.BYTES );
        long expectedFileBytes = expectedFullBytes + Long.BYTES; // because we are not on a byte boundary
        assertEquals( expectedFileBytes, file.length());
        int extraBits = LINE_COUNT -(expectedFullBytes * Byte.SIZE);
        long lastLong = (1L<<extraBits)-1;

        try( DataInputStream dib = new DataInputStream( new FileInputStream( file ))){
            for (int i=0;i<(int)expected_longs; i++)
            {
                assertEquals( ~0L, dib.readLong());
            }
            assertEquals( lastLong, dib.readLong());
            assertEquals( 0, dib.available());
        }
    }

    @Test
    public void loadTests() throws IOException {
        lines.clear();
        int bufferSizeInBytes = (Long.BYTES * shape.getNumberOfBits());
        URL url = TheDuelingMachineTests.class.getResource("/TheDuelingMachine.txt");
        BufferedReader reader = new BufferedReader( new InputStreamReader( url.openStream()));
        maxIdx = -1;
        int blocks = 1;
        while (reader.ready()) {
            String line = reader.readLine().trim();
            if (line.length() > 0) {
                lines.add( line );
                int idx = flatBloofi.add( asByteBuffer( line  ));
                maxIdx = maxIdx>idx?maxIdx:idx;
            }
            if (lines.size() / Long.SIZE == blocks && lines.size() % Long.SIZE == 0)
            {
                int expected = blocks*bufferSizeInBytes / Long.BYTES;
                File file = new File( dir, "BloomTable" );
                long fs = file.length();
                assertEquals( bufferSizeInBytes*blocks, file.length() );
                blocks++;
            }
        }
    }



}
