package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;

import com.instaclustr.cassandra.bloom.idx.mem.tables.BusyTable.CloseableIteratorOfInt;

import org.apache.commons.collections4.bloomfilter.BitMap;

/**
 * Manages a file that contains the offset of the index into a key file.
 *
 */
public class IdxTable extends AbstractTable implements AutoCloseable {

    private static final int ENTRY_SIZE = 3;

    class IdxEntry {
        IntBuffer buffer;
        int position;

        IdxEntry( IntBuffer buffer, int position ) {
            this.buffer = buffer;
            this.position = position;
        }

        int getPos() { return buffer.get(position); }
        void setPos( int pos ) {
            buffer.put(position+0,pos);
        }
        int getLen() { return buffer.get(position+1); }
        void setLen(int len) {
            buffer.put(position+1,len);
        }
        int getAlloc() { return buffer.get(position+3); }
        void setAlloc( int alloc ) {
            buffer.put( position+3, alloc );
        }
    }

//    /**
//     * heaer structure
//     * int blocksize
//     * int flags
//     */
//    private static final int HEADER_SIZE = 2;
//
//    private class Header {
//        IntBuffer buffer;
//        int getBlockSize() { return buffer.get(0); }
//        int getFlags() { return buffer.get(1); }
//    }


    public IdxTable(File bufferFile) throws IOException {
        super( bufferFile );
//        header = new Header();
//        if (getFileSize()==0) {
//            IntBuffer buffer = getWritableIntBuffer();
//            buffer.put(0, blockSize);
//            buffer.put(1, 0);
//        } else {
//            IntBuffer buffer = getIntBuffer();
//            if (buffer.get(0) != blockSize ) {
//                throw new IllegalArgumentException( String.format("IDX file already defiend with a block size of %s", buffer.get(0)));
//            }
//        }
    }

    @Override
    public String toString() {
        return "IdxTable: "+super.toString();
    }

    private int positionOf( int idx ) {
        return (idx*ENTRY_SIZE);
    }

    public IdxEntry get(int idx) throws IOException {
        IntBuffer buff = getWritableIntBuffer();
        int position = positionOf( idx );
        if (position > buff.limit()) {
            for (int i=0;i<ENTRY_SIZE;i++) {
                buff.put( position+i, 0 );
            }
        }
        return new IdxEntry( getIntBuffer(), positionOf( idx ) );
    }

}
