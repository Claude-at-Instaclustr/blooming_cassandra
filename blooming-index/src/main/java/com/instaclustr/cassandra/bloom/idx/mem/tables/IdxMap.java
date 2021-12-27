/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.IntConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table that maps the external index to the internal Buffer Table Idx
 *
 */
public class IdxMap extends BaseTable implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IdxMap.class);

    /**
     * The size of the Blocks
     */
    public static final int BLOCK_BYTES = 1 + Integer.BYTES;
    /**
     * The initialized flag.
     */
    private static final byte INITIALIZED_FLAG = (byte) 0x01;

    /**
     * An entry in the IdxMap table.
     */
    interface Entry {
        /**
         * Gets the block the entry is associated with.
         * @return the block the entry is associated with.
         */
        public int getBlock();

        /**
         * Gets the inisialized status.
         * @return {@code true} if the entry is initialized
         */
        public boolean isInitialized();

        /**
         * get the index of the BufferTableIdx.
         * @return the index of the buffer Table idx.
         */
        public int getKeyIdx();
    }

    /**
     * An Entry implementation that maps to the file.
     */
    class MapEntry implements Entry {
        /**
         * The buffer we are reading/writeing.
         */
        protected ByteBuffer buffer;
        /**
         * The block in the file that we are manipulating.
         */
        private int block;

        /**
         * Constructor.
         *
         * @param buffer the Buffer to read/write.
         * @param idx the block inn the file.
         */
        MapEntry(ByteBuffer buffer, int idx) {
            this.buffer = buffer;
            this.block = idx;
        }

        @Override
        public int getBlock() {
            return block;
        }

        /**
         * Set the Buffer Table idx value.
         * @param keyIdx the Buffer table idx value.
         * @throws IOException on IO Error.
         */
        public void setKeyIdx(int keyIdx) throws IOException {
            final ByteBuffer writeBuffer = getWritableBuffer();
            final int startByte = block * BLOCK_BYTES;
            sync(() -> writeBuffer.put(startByte, (byte) 1).putInt(startByte + 1, keyIdx), startByte, BLOCK_BYTES, 4);
        }

        @Override
        public boolean isInitialized() {
            return (buffer.get(block * BLOCK_BYTES) & INITIALIZED_FLAG) == INITIALIZED_FLAG;
        }

        @Override
        public int getKeyIdx() {
            return buffer.getInt((block * BLOCK_BYTES) + 1);
        }

        /**
         * Get a range lock on this Entry.
         * @param retryCount the number of times to retry the lock.
         * @return the RangeLock
         * @throws OutputTimeoutException if the lock could not be established.
         */
        public RangeLock lock(int retryCount) throws OutputTimeoutException {
            final int startByte = block * BLOCK_BYTES;
            return getLock(startByte, BLOCK_BYTES, retryCount);
        }
    }

    /**
     * An Entry implementation to search for deleted entries.
     */
    public static class SearchEntry implements Entry {
        /**
         * The block.  Defaults to Unset.
         * @see BufferTable.UNSET
         */
        private int block = BufferTable.UNSET;
        /**
         * The flag state we are looking for. Defaults to 0 (zero).
         */
        private byte flag = 0;
        /**
         * The BufferTable Idx value.  Defaults to Unset
         * @see BufferTable.UNSET
         */
        private int keyIdx = BufferTable.UNSET;

        /**
         * Constructor.
         */
        public SearchEntry() {
        }

        /**
         * Set the block value
         * @param block the block to search beyond.
         */
        public void setBlock(int block) {
            this.block = block;
        }

        @Override
        public int getBlock() {
            return this.block;
        }

        /**
         * The key idx to search for.
         * @param keyIdx the key idx.
         */
        public void setKeyIdx(int keyIdx) {
            this.keyIdx = keyIdx;
        }

        @Override
        public int getKeyIdx() {
            return keyIdx;
        }

        @Override
        public boolean isInitialized() {
            return (flag & INITIALIZED_FLAG) != INITIALIZED_FLAG;
        }

        /**
         * Set the initialized flag.
         * @param state
         */
        public void setInitialized(boolean state) {
            flag = state ? INITIALIZED_FLAG : 0;
        }

    }

    /**
     * Constructor.
     * @param bufferFile the File to read/write.
     * @throws IOException on IO Error.
     */
    public IdxMap(File bufferFile) throws IOException {
        this(bufferFile, BaseTable.READ_WRITE);
    }

    /**
     * Constructor.
     * @param bufferFile the File to read/write.
     * @param readOnly if {@code true} the table will be read only.
     * @throws IOException on IO Error.
     */
    public IdxMap(File bufferFile, boolean readOnly) throws IOException {
        super(bufferFile, BLOCK_BYTES, readOnly);
    }

    /**
     * Gets the map entry for the index.  Will create the index if it does not exist.
     * @param idx the index to read.
     * @return the Map Entry for the index.
     * @throws IOException on IO Error.
     */
    public MapEntry get(int idx) throws IOException {
        // ensure we have enough space for the block
        ensureBlock(idx + 1);
        ByteBuffer buff = getBuffer();
        return new MapEntry(buff, idx);
    }

    /**
     * Search for an index.
     * Conditions under which an entry will be passed to the consumer
     * <ul>
     * <li>If the target block matches the entry block.</li>
     * <li>If the target keyindex matches the target key index.</li>
     * <li>If the block is UNSET and both the target and index are uninitialized.</li>
     * </ul>
     * @param consumer the consumer to accept matching entries.
     * @param target the Search entry to look for.
     * @see BufferTable.UNSET
     */
    public void search(IntConsumer consumer, SearchEntry target) {
        try {
            int blocks = (int) getFileSize() / getBlockSize();
            for (int block = 0; block < blocks; block++) {
                try {
                    MapEntry entry = get(block);
                    if (target.getBlock() == entry.getBlock() || target.getKeyIdx() == entry.getKeyIdx()) {
                        consumer.accept(block);
                    } else if (target.getBlock() == BufferTable.UNSET && !target.isInitialized()
                            && !entry.isInitialized()) {
                        consumer.accept(block);
                    }
                } catch (IOException e) {
                    LOG.warn("Error during search {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            LOG.error("Error during search initialization {}", e.getMessage());
        }
    }

}
