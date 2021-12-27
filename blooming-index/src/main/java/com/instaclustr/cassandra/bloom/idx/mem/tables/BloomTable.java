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
import java.util.PrimitiveIterator;
import java.util.concurrent.Future;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.Func;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.RangeLock;

/**
 * This is the core of the Bloofi implementation.
 *
 * Each bit in the bloom filter is associated with a bit map indicating which
 * indexed filters have that bit enabled.  There is one bit map for each bit in the
 * shape of the filter being indexed.
 *
 * <p>See http://dx.doi.org/10.1016/j.is.2015.01.002 for a discussion of the Flat Blooi design</p>
 */
public class BloomTable implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BloomTable.class);

    /**
     * The number of bits in the Bloom filter.
     */
    private final int numberOfBits;
    /**
     * The buffer positioning calculator
     */
    private final BufferCalc bufferCalc;
    /**
     * The bit table that contains the bloom filters.
     */
    private final BitTable bitTable;

    /**
     * Constructor.  Opens the file in read/write mode.
     * @param numberOfBits the number of bits in the Bloom filters
     * @param file The file that contains the bloom filters.
     * @throws IOException on IO Error
     */
    public BloomTable(int numberOfBits, File file) throws IOException {
        this(numberOfBits, file, BaseTable.READ_WRITE);
    }

    /**
     * Constructor.
     * @param numberOfBits the number of bits in the Bloom filters
     * @param file The file that contains the bloom filters.
     * @param readOnly if {@code true} the table is read only, it is read/write otherwise.
     * @throws IOException on IO Error
     */
    public BloomTable(int numberOfBits, File file, boolean readOnly) throws IOException {
        this.bitTable = new BitTable(file, readOnly);
        this.numberOfBits = numberOfBits;
        this.bufferCalc = new BufferCalc();
        this.bitTable.setExtensionBlockSize(bufferCalc.lengthInBytes);
    }

    /**
     * Execute the func using the table executor.
     * @param fn the Func to execute
     * @return the Future for the func.
     * @see BaseTable#exec(Func)
     */
    public Future<?> exec(Func fn) {
        return bitTable.exec(fn);
    }

    /**
     * Sets the bloom filter at the index position in the table.
     * @param idx the index of the Bloom filter.
     * @param bloomFilter the Bloom filter
     * @throws IOException on IO Error
     */
    public void setBloomAt(int idx, LongBuffer bloomFilter) throws IOException {
        bitTable.ensureBlock(bufferCalc.getNumberOfBlocks(idx));

        LongBufferBitMap bloomBitMap = new LongBufferBitMap(bloomFilter);
        // for each enabled bit in the Bloom filter enable the idx bit in
        // the associated mappedBits.
        try {
            bitTable.retryOnTimeout(() -> {
                try (RangeLock lock = bitTable.getLock(bufferCalc.getBufferOffsetForIdx(idx),
                        bufferCalc.getLengthInBytes(), 4)) {
                    for (int i = 0; i < numberOfBits; i++) {
                        int bufferIdx = bufferCalc.getBufferBitPosition(idx, i);
                        if (bloomBitMap.isSet(i)) {
                            bitTable.set(bufferIdx);
                        } else {
                            bitTable.clear(bufferIdx);
                        }
                    }
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Error while trying to set Bloom filter", e);
        }
    }

    /**
     * Gets the Bloom filter at the index.
     * @param idx the index to retrieve.
     * @return the IndexProducer for the Bloom filter.
     */
    public IndexProducer getBloomAt(int idx) {
        return new IndexProducer() {

            @Override
            public boolean forEachIndex(IntPredicate predicate) {

                for (int i = 0; i < numberOfBits; i++) {
                    int bufferIdx = bufferCalc.getBufferBitPosition(idx, i);
                    try {
                        if (bitTable.retryOnTimeout(() -> bitTable.isSet(bufferIdx))) {
                            if (!predicate.test(i)) {
                                return false;
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Error when checking bit " + i, e);
                    }
                }
                return true;
            }
        };

    }

    /**
     * Search for the bloom filter in the table.
     * @param result an IntConsumer that will accept the indexes of the found filters.
     * @param bloomFilter the Bloom filter to search for.
     * @param busy the Bit Table associated with this Bloom table.
     * @throws IOException on IO Error
     */
    public void search(IntConsumer result, LongBuffer bloomFilter, BitTable busy) throws IOException {
        LongBufferBitMap bloomBitMap = new LongBufferBitMap(bloomFilter);
        ByteBuffer buffer = bitTable.getBuffer();

        int blockLimit = bufferCalc.getNumberOfBuffers();
        for (int blockIdx = 0; blockIdx < blockLimit; blockIdx++) {
            // positions in longs.
            long w = ~0L;
            PrimitiveIterator.OfInt bit = bloomBitMap.indices(numberOfBits);
            while (bit.hasNext()) {
                w &= buffer.getLong(bufferCalc.getBufferSearchPosition(blockIdx, bit.nextInt()));
            }

            while (w != 0) {
                long t = w & -w;
                int idx = Long.numberOfTrailingZeros(t) + (Long.SIZE * blockIdx);
                if (busy.isSet(idx)) {
                    result.accept(idx);
                }
                w ^= t;
            }
        }

    }

    @Override
    public String toString() {
        return bitTable.toString();
    }

    /**
     * Drops this table.  All data will be deleted.
     */
    public void drop() {
        BaseTable.closeQuietly(this);
        bitTable.drop();
    }

    @Override
    public void close() throws IOException {
        bitTable.close();
    }


    /**
     * For testing **NOT PART OF API***
     * @return the Underlying Bittable for this Bloom table.
     * @throws IOException on IO Error
     */
    BitTable getBitTable() throws IOException {
        return bitTable;
    }

    /**
     * Class to calculate buffer positions.
     */
    class BufferCalc {

        /**
         * each buffer entry accounts for 64 entries in the index. each there is one
         * long in each buffer entry for each bit in the bloom filter. each long is a
         * bit packed set of 64 flags, one for each entry.
         *
         * bufferEntryLength is the number of bits in the buffer entry.
         */
        private final int lengthInBytes = Long.BYTES * numberOfBits;

        /**
         * Gets the buffer entry length in bytes
         * @return the buffer length in bytes.
         */
        int getLengthInBytes() {
            return lengthInBytes;
        }

        /**
         * Gets the byte position in the file of the buffer entry containing idx.
         * @param idx the index to look for.
         * @return the byte position of the buffer entry containing idx.
         */
        public int getBufferOffsetForIdx(int idx) {
            return BitMap.getLongIndex(idx) * lengthInBytes;
        }

        /**
         * Gets the number of buffers in the file.
         * @return The number of buffers in the file
         * @throws IOException
         */
        public int getNumberOfBuffers() throws IOException {
            return (int) (bitTable.getFileSize() / lengthInBytes);
        }

        /**
         * Get the buffer positions for a bit in the buffer at idx.
         * @param idx the Item that we are looking for.
         * @param bit the bit [0,numberOfBit) that we are looking for.
         * @return the array of buffer positions for idx.
         */
        public int getBufferBitPosition(int idx, int bit) {
            // get the bufferEntry offset
            return (getBufferOffsetForIdx(idx) * Byte.SIZE)
                    // + the position of the bit block
                    + (bit * Long.SIZE)
                    // + the index of the idx in the bit block
                    + (idx % Long.SIZE); //
        }

        /**
         * Gets the number of low level block (Long.BYTES sized) blocks necessary to contain idx.
         * @param idx the index to contain.
         * @return the number of blocks necessary to contain idx.
         */
        public int getNumberOfBlocks(int idx) {
            int blockCount = BitMap.getLongIndex(idx) + 1;
            return blockCount;// * lengthInBytes / bitTable.getBlockSize();
        }

        /**
         * Gets the byte offset of the long in the buffer representing the bit
         * @param block the block number of the buffer
         * @param bit the bit number within the buffer.
         * @return the byte offset of the long in the buffer.
         */
        public int getBufferSearchPosition(int block, int bit) {
            return block * lengthInBytes + bit * Long.BYTES;
        }

    }

}
