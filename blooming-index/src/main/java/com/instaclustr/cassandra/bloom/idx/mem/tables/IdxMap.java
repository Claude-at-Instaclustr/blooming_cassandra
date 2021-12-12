package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Table that maps the external index to the internal KeytableIndex
 *
 */
public class IdxMap extends AbstractTable implements AutoCloseable {

    private static final int BLOCK_BYTES = 1 + Integer.BYTES;

    /**
     * Package private so that other classes in this package can use it.
     *
     */
    class MapEntry {
        private ByteBuffer buffer;
        private int idx;

        MapEntry(ByteBuffer buffer, int idx) {
            this.buffer = buffer;
            this.idx = idx;
        }

        public void setKeyIdx(int keyIdx) throws IOException {
            final ByteBuffer writeBuffer = getWritableBuffer();
            final int startByte = idx * BLOCK_BYTES;
            sync(() -> writeBuffer.put(startByte, (byte) 1).putInt(startByte + 1, keyIdx), startByte, BLOCK_BYTES, 4);
        }

        public boolean isInitialized() {
            return (buffer.get(idx * BLOCK_BYTES) & 0x01) > 0;
        }

        public int getKeyIdx() {
            return buffer.getInt((idx * BLOCK_BYTES) + 1);
        }
    }

    public IdxMap(File bufferFile) throws IOException {
        super(bufferFile, BLOCK_BYTES);

    }

    @Override
    public String toString() {
        return "IdxTable: " + super.toString();
    }

    public MapEntry get(int idx) throws IOException {
        // ensure we have enough space for the block

        ensureBlock(idx);
        ByteBuffer buff = getBuffer();
        return new MapEntry(buff, idx);
    }

}
