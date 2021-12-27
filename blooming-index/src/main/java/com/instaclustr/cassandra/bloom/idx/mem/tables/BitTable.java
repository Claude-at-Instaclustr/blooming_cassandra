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

/**
 * A class that implements an arry of bits as a table.
 * The table block size is the size of a Long value.
 */
public class BitTable extends BaseTable implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BitTable.class);

    /**
     * The write buffer.
     */
    private LongBuffer writeBuffer;

    /**
     * The lowest deleted  block.  When looking for deleted block the system will scan from this point.
     */
    private volatile int lowestDeletedBlock = 0;

    /**
     * Constructor.
     * @param busyFile The file to read/write.
     * @throws IOException on IO Error.
     */
    public BitTable(File busyFile) throws IOException {
        this(busyFile, BaseTable.READ_WRITE);
    }


    /**
     * Constructor.
     * @param busyFile The file to read/write.
     * @param readOnly if {@code true} the file will be read only, otherwise it is opend in read/write mode.
     * @throws IOException on IO Error.
     */
    public BitTable(File busyFile, boolean readOnly) throws IOException {
        super(busyFile, Long.BYTES, readOnly);
        if (readOnly) {
            writeBuffer = getLongBuffer();
        } else {
            super.registerExtendNotification(() -> {
                writeBuffer = getWritableLongBuffer();
            });
            writeBuffer = getWritableLongBuffer();
            executor.scheduleWithFixedDelay(() -> scanForLowest(), 0, 5, TimeUnit.MINUTES);
        }
    }

    /**
     * Scan for the lowest deleted  entry.  This scan starts at 0 and locates the first deleted bit.
     */
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

    /**
     * Get the maximum bit index.
     * @return the maximum bit index.
     * @throws IOException on IO Error.
     */
    public int getMaxIndex() throws IOException {
        return (int) getFileSize() * Byte.SIZE;
    }

    @Override
    public void close() throws IOException {
        writeBuffer = null;
        super.close();
    }

    /**
     * Class to scan the bit table locating all the disabled (unused) indexes.
     * The Callable implementation enables the bit current bit.  If there is no
     * current bit the next bit is located.
     */
    private class IndexScanner implements Callable<Boolean>, Iterator<Integer> {
        /**
         * The highest block to check.
         */
        private final int maxBlock;
        /**
         * The current block
         */
        private int block;
        /**
         * The mask that extracts the byte from the long
         */
        private long mask;
        /**
         * the current long from the file.
         */
        private long word;
        /**
         * The logical negation of the word
         */
        private long check;
        /**
         * the current file long that we are looking at.
         */
        private int blockIdx;
        /**
         * if not {@code null} contains the next available entry in the bit table.
         */
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

        /**
         * Checks that the check and mask
         * @return
         */
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

        /**
         * Finds the next match
         * @return {@code true} if a match was located, {@code false} otherwise.
         */
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
        } catch (IOException e) {
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
     * @see #sync(Callable, int, int, int)
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
     * @see #sync(Callable, int, int, int)
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
            long mask = BitMap.getLongBit(idx);
            return (buff.get(wordIdx) & mask) == mask;
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
