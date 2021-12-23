package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.LongBuffer;
import java.util.PrimitiveIterator;

import org.junit.Test;

public class LongBufferBitMapTest {

    @Test
    public void isSetTest() {
        int numberOfBits = Integer.SIZE + Long.SIZE;
        long t = ~0L;
        long t2 = t & 0XFFFFFFFF;
        LongBuffer lb = LongBuffer.wrap( new long[] { t, t2 } );
        LongBufferBitMap bitMap = new LongBufferBitMap( lb  );

        for (int i=0;i<numberOfBits;i++) {
            assertTrue( "Error at position "+i, bitMap.isSet(i) );
        }

        lb = LongBuffer.wrap( new long[] { 0L, 0L } );
        bitMap = new LongBufferBitMap( lb  );

        for (int i=0;i<numberOfBits;i++) {
            assertFalse( "Error at position "+i, bitMap.isSet(i) );
        }

    }

    @Test
    public void andTest() {
        LongBuffer lb = LongBuffer.wrap( new long[] { ~0L, ~0L } );
        LongBufferBitMap bitMap = new LongBufferBitMap( lb  );

        LongBuffer other = LongBuffer.wrap( new long[] { 0x0707070707070707L, 0x7070707070707070L } );
        LongBufferBitMap expected = new LongBufferBitMap( other );
        bitMap.and(other);

        for (int i=0;i<Long.SIZE*2;i++)
        {
            assertEquals( "Error at position "+i, expected.isSet(i), bitMap.isSet(i));
        }
    }

    @Test
    public void orTest() {
        LongBuffer lb = LongBuffer.wrap( new long[] { 0x0707070707070707L, 0x7070707070707070L } );
        LongBufferBitMap bitMap = new LongBufferBitMap( lb  );

        LongBuffer other =  LongBuffer.wrap( new long[] { 0x7171717171717171L, 0x1717171717171717L } );
        LongBufferBitMap expected = new LongBufferBitMap( LongBuffer.wrap( new long[] { 0x7777777777777777L, 0x7777777777777777L } ));
        bitMap.or(other);

        for (int i=0;i<Long.SIZE*2;i++)
        {
            assertEquals( "Error at position "+i, expected.isSet(i), bitMap.isSet(i));
        }
    }
    @Test
    public void xorTest() {
        LongBuffer lb = LongBuffer.wrap( new long[] { 0x0707070707070707L, 0x7070707070707070L } );
        LongBufferBitMap bitMap = new LongBufferBitMap( lb  );

        LongBuffer other =  LongBuffer.wrap( new long[] { 0x7171717171717171L, 0x1717171717171717L } );
        LongBufferBitMap expected = new LongBufferBitMap( LongBuffer.wrap( new long[] { 0x7676767676767676L, 0x6767676767676767L }) );
        bitMap.xor(other);

        for (int i=0;i<Long.SIZE*2;i++)
        {
            assertEquals( "Error at position "+i, expected.isSet(i), bitMap.isSet(i));
        }
    }
    @Test
    public void indiciesTest() {
        LongBuffer lb = LongBuffer.wrap( new long[] { 0xAAAAAAAAAAAAAAAAL, 0xAAAAAAAAAAAAAAAAL } );
        LongBufferBitMap bitMap = new LongBufferBitMap( lb  );

        PrimitiveIterator.OfInt iter = bitMap.indices( Long.SIZE*2 );
        int j=1;
        while (iter.hasNext() ) {
            int i = iter.nextInt();
            assertEquals( 1, i%2 );
            assertEquals( j, i );
            j+=2;
        }
    }

}
