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
import java.nio.LongBuffer;
import java.util.function.IntConsumer;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BloomTable extends BaseTable implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BloomTable.class);

    /**
     * The number of bits in the bloom filter.
     */
    private final int numberOfBits;

    /**
     * The sizes for a block (Long.SIZE) bloom filters
     */
    // private final int blockBytes;
    private final int blockWords;

    /**
     * Sizes of a bloom filter
     */
    // private final int filterBytes;
    public final int filterWords;

    /**
     * Get the block size in bytes.
     * @param numberOfBits the number of bits in the Bloom filter
     * @return the block size fot this table.
     */
    private static final int calcBlockSize(int numberOfBits) {
        // number of words * number of bytes in a long
        // the buffer is the number of bits in the Bloom filter x the number of bits in
        // a long.
        // but we round the number of bits in the bloom filter up by calculating the
        // word boundary.
        return (BitMap.numberOfBitMaps(numberOfBits) * Long.BYTES) * Long.BYTES;
    }

    /**
     * The sizes for a singe bloom filter
     * @param numberOfBits
     * @param bufferFile
     * @throws IOException
     */
    public BloomTable(int numberOfBits, File bufferFile) throws IOException {
        super(bufferFile, calcBlockSize(numberOfBits));

        this.numberOfBits = numberOfBits;

        filterWords = BitMap.numberOfBitMaps(numberOfBits);
        // filterBytes = filterWords * Long.BYTES;

        blockWords = filterWords * Long.BYTES;
        // blockBytes = filterBytes * Long.BYTES;

    }

    @Override
    public String toString() {
        return "BufferTable: " + super.toString();
    }

    private LongBuffer positionBuffer(LongBuffer buffer, int idx) {
        final int offset = BitMap.getLongIndex(idx) * blockWords;
        buffer.position(offset).limit(offset + blockWords);
        return buffer;
    }

    /**
     * Checks if the specified index bit is enabled in the array of bit bitmaps.
     *
     * If the bit specified by idx is not in the bitMap false is returned.
     *
     * @param bitMaps  The array of bit maps.
     * @param idx the index of the bit to locate.
     * @return {@code true} if the bit is enabled, {@code false} otherwise.
     */
    public static boolean contains(LongBuffer bitMaps, int idx) {
        int longIndex = BitMap.getLongIndex(idx);
        return idx >= 0 && longIndex < bitMaps.limit() && (bitMaps.get(longIndex) & BitMap.getLongBit(idx)) != 0;
    }

    public void setBloomAt(int idx, LongBuffer bloomFilter) throws IOException {
        // extract the proper block
        LongBuffer block = positionBuffer(getWritableLongBuffer(), idx);
        final long mask = BitMap.getLongBit(idx);

        final Func action = new Func() {
            @Override
            public void call() {
                for (int k = 0; k < numberOfBits; k++) {
                    long blockData = block.get(k);
                    if (contains(bloomFilter, k)) {
                        blockData |= mask;
                    } else {
                        blockData &= ~mask;
                    }
                    block.put(k, blockData);
                }
                // return null;
            }

        };

        sync(action, block.position() * Long.BYTES, block.limit() * Long.BYTES, 4);
    }

    public void search(IntConsumer result, LongBuffer bloomFilter, BusyTable busy) throws IOException {
        LongBuffer buffer = getLongBuffer();

        int blockLimit = buffer.remaining() / blockWords;
        for (int bockIdx = 0; bockIdx < blockLimit; bockIdx++) {

            int offset = bockIdx * blockWords;
            long w = ~0l;
            try (CloseableIteratorOfInt iter = new CloseableIteratorOfInt(bloomFilter)) {
                while (iter.hasNext()) {
                    w &= buffer.get(offset + iter.next());
                }
            } catch (Exception shouldNotHappen) {
                logger.error("Error on close of iterator", shouldNotHappen);
            }
            while (w != 0) {
                long t = w & -w;
                int idx = Long.numberOfTrailingZeros(t) + (Long.SIZE * bockIdx);
                if (busy.isSet(idx)) {
                    result.accept(idx);
                }
                w ^= t;
            }
        }

    }

}
