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

import com.instaclustr.cassandra.bloom.idx.mem.tables.BufferTableIdx.IdxEntry;

/**
 * Table that maps the external index to the internal KeytableIndex
 *
 */
public class IdxMap extends BaseTable implements AutoCloseable {

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

    public static void main(String[] args) throws IOException {
        File f = new File( args[0] );
        if (!f.exists()) {
            System.err.println( String.format( "%s does not exist", f.getAbsoluteFile() ));
        }
        System.out.println( "'Index','Initialized','reference'");
        try (IdxMap idx = new IdxMap( f )) {
            int blocks = (int)idx.getFileSize() / idx.getBlockSize();
            for (int block=0;block<blocks;block++) {
                MapEntry entry = idx.get(block);
                System.out.println( String.format("%s,%s,%s", block, entry.isInitialized(), entry.getKeyIdx()));
            }
        }
    }

    public IdxMap(File bufferFile) throws IOException {
        super(bufferFile, BLOCK_BYTES);
    }

    public MapEntry get(int idx) throws IOException {
        // ensure we have enough space for the block

        ensureBlock(idx + 1);
        ByteBuffer buff = getBuffer();
        return new MapEntry(buff, idx);
    }

}
