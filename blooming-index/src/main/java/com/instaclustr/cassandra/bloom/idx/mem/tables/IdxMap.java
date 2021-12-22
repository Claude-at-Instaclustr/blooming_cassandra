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
 * Table that maps the external index to the internal KeytableIndex
 *
 */
public class IdxMap extends BaseTable implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IdxMap.class);

    public static final int BLOCK_BYTES = 1 + Integer.BYTES;

    interface Entry {
        public int getBlock();

        public boolean isInitialized();

        public int getKeyIdx();
    }

    class MapEntry implements Entry {
        protected ByteBuffer buffer;
        private int block;

        MapEntry(ByteBuffer buffer, int idx) {
            this.buffer = buffer;
            this.block = idx;
        }

        @Override
        public int getBlock() {
            return block;
        }

        public void setKeyIdx(int keyIdx) throws IOException {
            final ByteBuffer writeBuffer = getWritableBuffer();
            final int startByte = block * BLOCK_BYTES;
            sync(() -> writeBuffer.put(startByte, (byte) 1).putInt(startByte + 1, keyIdx), startByte, BLOCK_BYTES, 4);
        }

        @Override
        public boolean isInitialized() {
            return (buffer.get(block * BLOCK_BYTES) & 0x01) > 0;
        }

        @Override
        public int getKeyIdx() {
            return buffer.getInt((block * BLOCK_BYTES) + 1);
        }

        public RangeLock lock(int retryCount) throws OutputTimeoutException {
            final int startByte = block * BLOCK_BYTES;
            return getLock(startByte, BLOCK_BYTES, retryCount);
        }
    }

    public static class SearchEntry implements Entry {
        private int block = BufferTable.UNSET;
        private byte flag = 0;
        private int keyIdx = BufferTable.UNSET;

        public SearchEntry() {
        }

        public void setBlock(int block) {
            this.block = block;
        }

        @Override
        public int getBlock() {
            return this.block;
        }

        public void setKeyIdx(int keyIdx) {
            this.keyIdx = keyIdx;
        }

        @Override
        public int getKeyIdx() {
            return keyIdx;
        }

        @Override
        public boolean isInitialized() {
            return (flag & 0x01) > 0;
        }

        public void setInitialized(boolean state) {
            flag = (byte) (state ? 0x01 : 0);
        }

    }

    public static void main(String[] args) throws IOException {
        File f = new File(args[0]);
        if (!f.exists()) {
            System.err.println(String.format("%s does not exist", f.getAbsoluteFile()));
        }
        System.out.println("'Index','Initialized','reference'");
        try (IdxMap idx = new IdxMap(f, BaseTable.READ_ONLY)) {
            int blocks = (int) idx.getFileSize() / idx.getBlockSize();
            for (int block = 0; block < blocks; block++) {
                MapEntry entry = idx.get(block);
                System.out.println(String.format("%s,%s,%s", block, entry.isInitialized(), entry.getKeyIdx()));
            }
        }
    }

    public IdxMap(File bufferFile) throws IOException {
        this(bufferFile, BaseTable.READ_WRITE);
    }

    public IdxMap(File bufferFile, boolean readOnly) throws IOException {
        super(bufferFile, BLOCK_BYTES, readOnly);
    }

    public MapEntry get(int idx) throws IOException {
        // ensure we have enough space for the block

        ensureBlock(idx + 1);
        ByteBuffer buff = getBuffer();
        return new MapEntry(buff, idx);
    }

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
