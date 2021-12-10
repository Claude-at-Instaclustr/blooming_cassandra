package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.concurrent.TimeoutException;

/**
 * Manages a file that contains the offset of the index into a key file.
 *
 */
public class IdxTable extends AbstractTable implements AutoCloseable {

    private static final int BLOCK_INTS = 3;

    /**
     * Package private so that other classes in this package can use it.
     *
     */
    class IdxEntry {
        IntBuffer buffer;
        int block;

        IdxEntry(IntBuffer buffer, int block) {
            this.buffer = buffer;
            this.block = block;
        }

        private void doPut( int blockOffset, int value) throws IOException {
            final IntBuffer writeBuffer = getWritableIntBuffer();
            final int startInt = (block*BLOCK_INTS)+blockOffset;
            final int startByte = startInt * Integer.BYTES;
            try {
                sync( ()-> writeBuffer.put( startInt, value ), startByte, Integer.BYTES, 4 );
            } catch (TimeoutException e) {
                throw new IOException( e );
            }
        }

        int getPos() {
            return buffer.get(block*BLOCK_INTS);
        }


        void setPos(int pos) throws IOException {
            doPut( 0, pos );
        }

        int getLen() {
            return buffer.get(block*BLOCK_INTS + 1);
        }

        void setLen(int len) throws IOException {
            doPut( 1, len );
        }

        int getAlloc() {
            return buffer.get(block*BLOCK_INTS+2);
        }

        void setAlloc(int alloc) throws IOException {
            doPut( 2, alloc );
        }
    }

    public IdxTable(File bufferFile) throws IOException {
        super(bufferFile, BLOCK_INTS * Integer.BYTES );

    }

    @Override
    public String toString() {
        return "IdxTable: " + super.toString();
    }

    public IdxEntry get(int block) throws IOException {
        IntBuffer buff = getIntBuffer();
        // ensure we have enough space for the block
        ensureBlock( block );

        return new IdxEntry(buff, block);
    }

}
