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
import java.util.PrimitiveIterator;
import java.util.function.IntConsumer;
import org.apache.commons.collections4.bloomfilter.BitMap;
import com.instaclustr.cassandra.bloom.idx.mem.LongBufferBitMap;

/**
 * This is the core of the Bloofi implementation.
 *
 * Each bit in the bloom filter is associated with a bit map indicating which
 * indexed filters have that bit enabled.  There is one bit map for each bit in the
 * shape of the filter being indexed.
 */
public class BloomTable extends BusyTable {

    /**
     * The number of bits in the Bloom filter.
     */
    private final int numberOfBits;
    /**
     * each buffer entry accounts for 64 entries in the index. each there is one
     * long in each buffer entry for each bit in the bloom filter. each long is a
     * bit packed set of 64 flags, one for each entry.
     *
     * bufferEntryLength is the number of bits in the buffer entry.
     */
    private final int bufferEntryLength;

    /**
     * The sizes for a singe bloom filter
     * @param numberOfBits
     * @param bufferFile
     * @throws IOException
     */
    public BloomTable(int numberOfBits, File file) throws IOException {
        super(file);
        this.numberOfBits = numberOfBits;
        this.bufferEntryLength = Long.SIZE * numberOfBits;

    }

    /**
     * Gets the byte position of the buffer entry containing idx.
     * @param idx the index to look for.
     * @return the byte position of the buffer entry containing idx.
     */
    private int getBufferOffset(int idx) {
        return BitMap.getLongIndex(idx) * bufferEntryLength;
    }

    /**
     * Get the buffer positions for each bit in the buffer at idx.
     * @param idx the Item that we are looking for.
     * @param bit the bit [0,numberOfBit) that we are looking for.
     * @return the array of buffer positions for idx.
     */
    private int getBufferIndex(int idx, int bit) {
        // get the bufferEntry offset
        return getBufferOffset(idx)
                // + the position of the bit block
                + (bit * Long.SIZE)
                // + the index of the idx into the buffer
                + idx; //
    }

    /**
     * Gets the number of blocks necessary to contain idx.
     * @param idx the index to contain.
     * @return the number of blocks necessary to contain idx.
     */
    private int getNumberOfBlocks(int idx) {
        return BitMap.numberOfBitMaps(idx + 1) * bufferEntryLength / getBlockSize();
    }

    /**
     * Sets the bloom filter at the index position in the table.
     * @param idx the index of the Bloom filter.
     * @param bloomFilter the Bloom filter
     * @throws IOException on Error
     */
    public void setBloomAt(int idx, LongBuffer bloomFilter) throws IOException {
        ensureBlock(getNumberOfBlocks(idx));

        LongBufferBitMap bloomBitMap = new LongBufferBitMap(bloomFilter);
        // for each enabled bit in the Bloom filter enable the idx bit in
        // the associated mappedBits.
        for (int i = 0; i < numberOfBits; i++) {
            int bufferIdx = getBufferIndex(idx, i);
            if (bloomBitMap.isSet(i)) {
                set(bufferIdx);
            } else {
                clear(bufferIdx);
            }
        }
    }

    private int getNumberOfBufferEnties() throws IOException {
        return (int) (getFileSize() / bufferEntryLength);
    }

    /**
     * Search for the bloom filter in the table.
     * @param result an IntConsumer that will accept the indexes of the found filters.
     * @param bloomFilter the Bloom filter to search for.
     * @param busy the Busy table associated with this Bloom table.
     * @throws IOException on IO Error
     */
    public void search(IntConsumer result, LongBuffer bloomFilter, BusyTable busy) throws IOException {
        LongBufferBitMap bloomBitMap = new LongBufferBitMap(bloomFilter);
        LongBuffer buffer = getLongBuffer();

        int bufferEntryLimit = getNumberOfBufferEnties();
        for (int bufferEntryIdx = 0; bufferEntryIdx < bufferEntryLimit; bufferEntryIdx++) {
            // positions in longs.
            int entryPosition = bufferEntryIdx * bufferEntryLength;
            long w = ~0L;
            PrimitiveIterator.OfInt wordInBufferEntry = bloomBitMap.indices(numberOfBits);
            while (wordInBufferEntry.hasNext()) {
                int iterPos = entryPosition + wordInBufferEntry.next();
                long entry = buffer.get(iterPos);
                System.out.println(entry);
                w &= buffer.get(iterPos);
            }

            while (w != 0) {
                long t = w & -w;
                int idx = Long.numberOfTrailingZeros(t) + (Long.SIZE * bufferEntryIdx);
                if (busy.isSet(idx)) {
                    result.accept(idx);
                }
                w ^= t;
            }
        }

    }
}
