package com.instaclustr.cassandra.bloom.idx.mem;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BitMapProducer.ArrayBuilder;
import org.junit.Test;

public class ByteBufferBitMapProducerTest {


    public ByteBufferBitMapProducerTest() {
    }

    @Test
    public void testOnePlus() {
        byte[] raw = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A };
        ByteBuffer bb = ByteBuffer.wrap( raw );
        List<Long> actual = new ArrayList<Long>();
        BitMapProducer producer = new ByteBufferBitMapProducer( bb );

        producer.forEachBitMap( actual::add );

        assertEquals( 2, actual.size() );
        assertEquals( Long.valueOf(0x0102030405060708L), actual.get(0) );
        assertEquals( Long.valueOf(0x090AL), actual.get(1) );

    }

    @Test
    public void testLessThanOne() {
        byte[] raw = { 0x09, 0x0A };
        ByteBuffer bb = ByteBuffer.wrap( raw );
        List<Long> actual = new ArrayList<Long>();
        BitMapProducer producer = new ByteBufferBitMapProducer( bb );

        producer.forEachBitMap( actual::add );

        assertEquals( 1, actual.size() );
        assertEquals( Long.valueOf(0x090AL), actual.get(0) );

    }
}
