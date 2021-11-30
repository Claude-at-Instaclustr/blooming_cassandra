package com.instaclustr.cassandra.bloom.idx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.TreeSet;

import org.junit.Test;

public class IndexKeyTest {


    @Test
    public void testOrdering() {
        IndexKey k305 = new IndexKey( 3, 0x05 );
        IndexKey k2ff = new IndexKey( 2, 0xFF );
        IndexKey k1ff = new IndexKey( 1, 0xFF );
        IndexKey k200 = new IndexKey( 2, 0x00 );

        TreeSet<IndexKey> ts = new TreeSet<IndexKey>();
        ts.add( k305 );
        ts.add( k2ff );
        ts.add( k1ff );
        ts.add( k200 );

        Iterator<IndexKey> iter = ts.iterator();
        assertEquals( k1ff, iter.next() );
        assertEquals( k2ff, iter.next() );
        assertEquals( k305, iter.next() );
        assertEquals( k200, iter.next() );

    }

    @Test
    public void isZeroTest() {
        assertFalse( new IndexKey( 3, 0x05 ).isZero() );
        assertFalse( new IndexKey( 0, 0x05 ).isZero() );
        assertTrue( new IndexKey( 3, 0 ).isZero() );

    }

}
