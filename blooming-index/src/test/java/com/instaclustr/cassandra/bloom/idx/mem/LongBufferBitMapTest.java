package com.instaclustr.cassandra.bloom.idx.mem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.PrimitiveIterator;

import org.junit.Test;

public class LongBufferBitMapTest {

    LongBuffer left = LongBuffer.allocate( 5 );
    LongBuffer right = LongBuffer.allocate( 5 );
    LongBufferBitMap bitMap = new LongBufferBitMap( left );


    @Test
    public void andTest() {
        left.put(0, 0x11L );
        left.put(1, 0x22L );
        left.put(2, 0x33L );
        left.put(3, 0x44L );
        left.put(4, 0x55L );

        right.put(0, 0x10L );
        right.put(1, 0x20L );
        right.put(2, 0x30L );
        right.put(3, 0x40L );
        right.put(4, 0x50L );

        bitMap.and( right );

        assertEquals( 0x10L, left.get(0));
        assertEquals( 0x20L, left.get(1));
        assertEquals( 0x30L, left.get(2));
        assertEquals( 0x40L, left.get(3));
        assertEquals( 0x50L, left.get(4));

    }

    @Test
    public void isSetTest() {
        left.put(0, 0x1L );
        left.put(1, 0x2L );
        left.put(2, 0x3L );
        left.put(3, 0x4L );
        left.put(4, 0x5L );

        Integer[] trueValues = { 0, 65, 128, 129, 194, 256, 258 };
        List<Integer> trueVal = Arrays.asList(trueValues);
        for (int i=0;i<Long.SIZE*5;i++) {
            if (trueVal.contains(i)) {
                assertTrue( "bad value at "+i, bitMap.isSet( i ));
            } else {
                assertFalse( "bad value at "+i, bitMap.isSet( i ) );
            }
        }
        assertFalse( bitMap.isSet( Long.SIZE*5  ) );
        assertThrows(IndexOutOfBoundsException.class, () -> bitMap.isSet( -1 ) );
    }

    @Test
    public void orTest() {
        LongBufferBitMap bitMap = new LongBufferBitMap( left );
        left.put(0, 0x1L );
        left.put(1, 0x2L );
        left.put(2, 0x3L );
        left.put(3, 0x4L );
        left.put(4, 0x5L );

        right.put(0, 0x10L );
        right.put(1, 0x20L );
        right.put(2, 0x30L );
        right.put(3, 0x40L );
        right.put(4, 0x50L );

        bitMap.or( right );

        assertEquals( 0x11L, left.get(0));
        assertEquals( 0x22L, left.get(1));
        assertEquals( 0x33L, left.get(2));
        assertEquals( 0x44L, left.get(3));
        assertEquals( 0x55L, left.get(4));
    }

    @Test
    public void xorTest() {

        left.put(0, 0x1L );
        left.put(1, 0x2L );
        left.put(2, 0x3L );
        left.put(3, 0x4L );
        left.put(4, 0x5L );

        right.put(0, 0x11L );
        right.put(1, 0x21L );
        right.put(2, 0x31L );
        right.put(3, 0x41L );
        right.put(4, 0x51L );

        bitMap.xor( right );

        assertEquals( 0x10L, left.get(0));
        assertEquals( 0x23L, left.get(1));
        assertEquals( 0x32L, left.get(2));
        assertEquals( 0x45L, left.get(3));
        assertEquals( 0x54L, left.get(4));

    }

    @Test
    public void indiciesTest() {
        left.put(0, 0x1L );
        left.put(1, 0x2L );
        left.put(2, 0x3L );
        left.put(3, 0x4L );
        left.put(4, 0x5L );

        int[] trueValues = { 0, 65, 128, 129, 194, 256, 258 };
        PrimitiveIterator.OfInt iter = bitMap.indices( 5*Long.SIZE );

        for (int value : trueValues) {
            assertTrue( iter.hasNext() );
            assertEquals( value, iter.nextInt() );
        }

        int[] partialValues = { 0, 65, 128, 129, 194 };
        iter = bitMap.indices( 4*Long.SIZE );

        for (int value : partialValues) {
            assertTrue( iter.hasNext() );
            assertEquals( value, iter.nextInt() );
        }

    }

}
