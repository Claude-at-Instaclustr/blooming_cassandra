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
package com.instaclustr.cassandra.bloom.idx.mem;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.function.IntConsumer;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.mem.tables.BloomTable;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BitTable;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndexer;

/**
 * This is what Daniel Lemire called Bloofi2. Basically, instead of using a tree
 * structure like Bloofi, we "transpose" the BitSets.
 */
public final class FlatBloofi implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BloomingIndexer.class);

    /*
     * each buffer entry accounts for 64 entries in the index. each there is one
     * long in each buffer entry for each bit in the bloom filter. each long is a
     * bit packed set of 64 flags, one for each entry.
     */
    private final BitTable busy;
    private final BloomTable buffer;
    private final int numberOfBits;

    public FlatBloofi(File dir, int numberOfBits) throws IOException {
        this.numberOfBits = numberOfBits;
        buffer = new BloomTable(numberOfBits, new File(dir, "BloomTable"));
        try {
            busy = new BitTable(new File(dir, "BusyTable"));
        } catch (IOException e) {
            BaseTable.closeQuietly(buffer);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            busy.close();
        } catch (IOException e) {
            BaseTable.closeQuietly(buffer);
            throw e;
        }
        buffer.close();
    }

    public int add(ByteBuffer bloomFilter) throws IOException {
        int idx = busy.newIndex();
        try {
            buffer.setBloomAt(idx, bloomFilter.asLongBuffer());
            return idx;
        } catch (IOException e) {
            try {
                busy.clear(idx);
            } catch (IOException ignore) {
                logger.error("Error when trying to clear index", e);
            }
            throw e;
        }
    }

    public void update(int idx, ByteBuffer bloomFilter) throws IOException {
        buffer.setBloomAt(idx, bloomFilter.asLongBuffer());
    }

    private LongBuffer adjustBuffer(ByteBuffer bloomFilter) {
        if (bloomFilter.remaining() == BitMap.numberOfBitMaps(numberOfBits) * Long.BYTES) {
            return bloomFilter.asLongBuffer().asReadOnlyBuffer();
        }
        if (bloomFilter.remaining() > BitMap.numberOfBitMaps(numberOfBits) * Long.BYTES) {
            throw new IllegalArgumentException("Bloom filter is too long");
        }
        // must be shorter

        byte[] buff = new byte[BitMap.numberOfBitMaps(numberOfBits) * Long.BYTES];

        bloomFilter.get(buff, bloomFilter.position(), bloomFilter.remaining());
        return ByteBuffer.wrap(buff).asLongBuffer().asReadOnlyBuffer();
    }

    public void search(IntConsumer result, ByteBuffer bloomFilter) throws IOException {

        buffer.search(result, adjustBuffer(bloomFilter), busy);
    }

    public void delete(int idx) throws IOException {
        busy.clear(idx);
    }

    public int count() throws IOException {
        return busy.cardinality();
    }

}
