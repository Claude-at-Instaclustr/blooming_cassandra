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
import java.nio.LongBuffer;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BitTable extends BaseTable implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BitTable.class);

    private LongBuffer writeBuffer;

    private volatile int lowestDeletedBlock = 0;

    public BitTable(File busyFile) throws IOException {
        super(busyFile, Long.BYTES);
        super.registerExtendNotification(() -> {
            writeBuffer = getWritableLongBuffer();
        });
        writeBuffer = getWritableLongBuffer();
        executor.scheduleWithFixedDelay(() -> scanForLowest(), 0, 5, TimeUnit.MINUTES);
    }

    private void scanForLowest() {
        LongBuffer buff = writeBuffer.duplicate();
        long full = ~0L;
        for (int i = 0; i < buff.limit(); i++) {
            lowestDeletedBlock = i;
            if (buff.get() != full) {
                return;
            }
        }
    }

    @Override
    public void close() throws IOException {
        writeBuffer = null;
        super.close();
    }

    /**
     * Class to scan the busy table locating all the disabled (unused) indexes.
     * The Callable implementation enables the bit current bit.  If there is no
     * current bit the next bit is located.
     */
    private class IndexScanner implements Callable<Boolean>, Iterator<Integer> {
        private final int maxBlock;
        private int block;
        private long mask;
        private long word;
        private long check;
        private int blockIdx;
        private Integer next;

        /**
         * Constructor.
         * @param maxBlock the highest block to check.
         */
        IndexScanner(int maxBlock) {
            this.maxBlock = maxBlock;
            this.block = lowestDeletedBlock;
            this.next = null;
        }

        private boolean matches() {
            return (check & mask) != 0;
        }

        /**
         * Get the block that the scanner is looking at.
         * @return
         */
        public int getBlock() {
            return block;
        }

        @Override
        public Boolean call() {
            if (hasNext() && matches()) {
                writeBuffer.put(block, word | mask);
                return true;
            }
            return false;
        }

        private boolean findMatch() {
            while (block < maxBlock) {
                this.word = writeBuffer.get(block);
                if (this.word != ~0L) {
                    this.check = word ^ ~0L; // convert all 0 to 1 and visa versa
                    while (blockIdx < Long.SIZE) {
                        mask = BitMap.getLongBit(blockIdx);
                        if (matches()) {
                            if (lowestDeletedBlock < block) {
                                lowestDeletedBlock = block;
                            }
                            return true;
                        }
                        blockIdx++;
                    }
                }
                block++;
                if (block < lowestDeletedBlock) {
                    block = lowestDeletedBlock;
                }
            }
            return false;
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                if (findMatch()) {
                    next = Integer.valueOf((block * Long.SIZE) + blockIdx);
                }
            }
            return next != null;
        }

        @Override
        public Integer next() {
            try {
                return next;
            } finally {
                next = null;
            }

        }
    }

    /**
     * Returns an unused index value.
     * Will locate deleted indexes and use them first.
     * @return the index number.
     * @throws IOException on IO Error
     */
    public int newIndex() throws IOException {
        IndexScanner scanner = new IndexScanner(writeBuffer.limit());
        try {
            while (scanner.hasNext()) {
                try {
                    if (sync(scanner, scanner.getBlock() * Long.BYTES, Long.BYTES, 4)) {
                        return scanner.next();
                    }
                } catch (OutputTimeoutException e) {
                    logger.warn("newIndex timeout: {} trying again", e.getMessage());
                    // skip the entry
                    scanner.next();
                }
            }
        }catch (IOException e) {
            logger.warn("newIndex failure: {}, creating new entry", e.getMessage());
        }
        // there is no old index so create a new one.
        try (RangeLock lock = getLock(extendBuffer(), Long.BYTES, 4)) {
            ByteBuffer buff = getWritableBuffer();
            // should be updated by extend notification writeBuffer = buff.asLongBuffer();
            buff.putLong(lock.getStart(), 1l);
            return lock.getStart() * Byte.SIZE;
        }
    }

    /**
     * Clears a set bit.
     * @param idx the bit to set.
     * @throws IOException on IOError
     * @throws IndexOutOfBoundx exception if {@code idx<0}.
     */
    public void clear(int idx) throws IOException {
        checkGEZero(idx, "index");
        int wordIdx = BitMap.getLongIndex(idx);
        long mask = BitMap.getLongBit(idx);
        sync(() -> writeBuffer.put(wordIdx, writeBuffer.get(wordIdx) & ~mask), wordIdx * Long.BYTES, Long.BYTES, 4);
        if (wordIdx < lowestDeletedBlock) {
            lowestDeletedBlock = wordIdx;
        }
    }

    /**
     * Sets a bit.
     * @param idx the bit to set.
     * @throws IOException on IOError
     * @throws IndexOutOfBoundx exception if {@code idx<0}.
     */
    public void set(int idx) throws IOException {
        checkGEZero(idx, "index");
        int wordIdx = BitMap.getLongIndex(idx);
        sync(() -> writeBuffer.put(wordIdx, writeBuffer.get(wordIdx) | BitMap.getLongBit(idx)), wordIdx * Long.BYTES,
                Long.BYTES, 4);

    }

    /**
     * Checks if a bit is set.
     * @param idx the bit to check.
     * @return {@code true } if the bit was set.
     * @throws IOException on IOError
     * @throws IndexOutOfBoundx exception if {@code idx<0}.
     */
    public boolean isSet(int idx) throws IOException {
        checkGEZero(idx, "index");
        LongBuffer buff = getLongBuffer();
        try {
            int wordIdx = BitMap.getLongIndex(idx);
            return (buff.get(wordIdx) & BitMap.getLongBit(idx)) > 0;
        } catch (IndexOutOfBoundsException exception) {
            // looked beyond buffer so not set.
            return false;
        }
    }

    /**
     * Gets the cardinality of the table.
     * @return the number of bits that are on.
     * @throws IOException on IO error.
     */
    public int cardinality() throws IOException {
        LongBuffer buff = getLongBuffer();
        int result = 0;
        while (buff.hasRemaining()) {
            result += Long.bitCount(buff.get());
        }
        return result;
    }
}
