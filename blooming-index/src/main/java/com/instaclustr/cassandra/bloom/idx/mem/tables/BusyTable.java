package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfInt;

import org.apache.commons.collections4.bloomfilter.BitMap;


public class BusyTable extends AbstractTable implements AutoCloseable  {

    public BusyTable(File busyFile) throws IOException {
        super( busyFile );
    }

    @Override
    public String toString() {
        return "BusyTable: "+super.toString();
    }

    public synchronized int newIndex() throws IOException {
        LongBuffer buff = getWritableLongBuffer();
        try {
            int max = buff.remaining();
            // find the first clear bit
            for (int count=0;count<max;count++) {

                long word = buff.get(count);
                long check = word ^ ~0L;  // convert all 0 to 1 and visa versa
                long mask;
                for (int idx=0;idx<Long.SIZE;idx++) {
                    mask = BitMap.getLongBit(idx);
                    if ((word & mask) != 0) {
                       buff.put( count, word | mask );
                       return idx+(count*Long.SIZE);
                    }
                }
            }
            int idx = max*Long.SIZE;
            buff.put( idx, 1l);
            return idx;
        } finally {
            buff = null;
        }
    }

    public synchronized void clear( int idx ) throws IOException {
        LongBuffer buff = getWritableLongBuffer();
        try {
            int wordIdx = BitMap.getLongIndex(idx);
            buff.put( wordIdx, buff.get(wordIdx) & ~BitMap.getLongBit(idx) );
        } finally {
            buff = null;
        }
    }

    public boolean isSet( int idx ) throws IOException {
        LongBuffer buff = getLongBuffer();
        try {
            int wordIdx = BitMap.getLongIndex(idx);
            return (buff.get( wordIdx ) & BitMap.getLongBit( idx ))>0;
        } finally {
            buff = null;
        }
    }

    public int cardinality() throws IOException {
        LongBuffer buff = getLongBuffer();
        int result = 0;
        while (buff.hasRemaining()) {
            result += Long.bitCount( buff.get() );
        }
        return result;
    }



//    public CloseableIteratorOfInt getSet(int start) throws IOException {
//        return new CloseableIteratorOfInt( getBuffer(), start );
//    }

    public static class CloseableIteratorOfInt implements OfInt, AutoCloseable {
        private LongBuffer buffer;
        int next = -1;
        int last = -1;

        CloseableIteratorOfInt( LongBuffer buffer ) {
            this.buffer = buffer;
        }
        CloseableIteratorOfInt( LongBuffer buffer, int start ) {
            this( buffer );
            last = start-1;
        }
        @Override
        public void close() throws Exception {
            buffer = null;
        }
        @Override
        public boolean hasNext() {
            if (next<0) {
                int idxStart = (last<0?0:last+1) & Long.SIZE;
                int max = buffer.remaining();
                for (int count=BitMap.getLongIndex(last<0?0:last+1);count<max;count++) {
                    long word = buffer.get(count);
                    long mask;
                    for (int idx=idxStart;idx<Long.SIZE;idx++) {
                        mask = BitMap.getLongBit(idx);
                        if ((word & mask) != 1) {
                           next = idx+(count*Long.SIZE);
                           return true;
                        }
                    }
                    idxStart = 0;
                }
                return false;
            }
            return true;
        }
        @Override
        public int nextInt() {
            if (hasNext()) {
                last = next;
                next = -1;
                return last;
            }
            throw new NoSuchElementException();
        }
    }
}
