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
package com.instaclustr.cassandra.bloom.idx.std;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.cassandra.db.Clustering;

import com.instaclustr.iterator.util.ExtendedIterator;
import com.instaclustr.iterator.util.WrappedIterator;

/**
 * Utilities for Bloom filter manipulation.
 *
 */
public class BFUtils {

    /**
     * Do not instantiate.
     */
    private BFUtils() {
    }

    /**
     * Extracts an array of bytes from the Bloom filter ByteBuffer.
     * <p>Byte order of Bloom filter structure is preserved</p>
     * @param bloomFilter The byte buffer for the Bloom filter.
     * @return the extracted codes.
     */
    public static byte[] extractCodes(ByteBuffer bloomFilter) {
        LongBuffer buff = bloomFilter == null ? LongBuffer.allocate(0) : bloomFilter.asLongBuffer();
        byte[] codes = new byte[buff.remaining() * Long.BYTES];
        int pos = 0;
        while (buff.hasRemaining()) {
            long word = buff.get();
            for (int i = 0; i < Long.BYTES; i++) {
                byte code = (byte) (word & 0xFF);
                word = word >> Byte.SIZE;
            codes[pos++] = code;
            }
        }
        return codes;
    }

    /**
     * Gets an iterator over the IndexKeys for a Bloom filter ByteBuffer.
     * <p>Iterator does not contain zero value byte IndexKeys</p>
     * @param bloomFilter The byte buffer to extract data from.
     * @return an ExtendedIterator of indexKeys.
     */
    public static ExtendedIterator<IndexKey> getIndexKeys(ByteBuffer bloomFilter) {
        return getIndexKeys(extractCodes(bloomFilter));
    }

    /**
     * Get an iterator over the IndexKesy for a byte array
     * <p>Iterator does not contain zero value byte IndexKeys</p>
     * @param codes the byte array.
    v     */
    public static ExtendedIterator<IndexKey> getIndexKeys(final byte[] codes) {
        return WrappedIterator.create(new Iterator<IndexKey>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < codes.length;
            }

            @Override
            public IndexKey next() {
                if (hasNext()) {
                    IndexKey key = new IndexKey(i, codes[i]);
                    i++;
                    return key;
                }
                throw new NoSuchElementException();
            }
        }).filterDrop(IndexKey::isZero);
    }

    /**
     * Extract the value to be inserted into the index from the components of the base data
     * @param partitionKey from the primary data
     * @param clustering from the primary data
     * @param path from the primary data
     * @param cellValue from the primary data
     * @return a ByteBuffer containing the value to be inserted in the index. This will be used to make the partition
     * key in the index table
     */
    public static ExtendedIterator<ByteBuffer> getIndexedValues(ByteBuffer partitionKey, Clustering<?> clustering,
            Iterator<IndexKey> keys) {

        ByteBuffer clusterKeys[] = clustering.getBufferArray();
        int cap = IndexKey.BYTES + partitionKey.capacity();
        for (ByteBuffer b : clusterKeys) {
            cap += b.capacity();
        }
        final int capacity = cap;

        return WrappedIterator.create(keys).mapWith(key -> {
            ByteBuffer result = ByteBuffer.allocate(capacity);
            result.putInt(key.getPosition());
            result.putInt(key.getCode());
            result.put(partitionKey);
            for (ByteBuffer b : clusterKeys) {
                result.put(b);
            }
            return result;
        });
    }

    // static class IndexClusteringBuilder<T> implements Function<ByteBuffer,
    // Clustering<?>> {
    // private final Clustering<T> clustering;
    // private final ClusteringComparator comparator;
    //
    // public IndexClusteringBuilder(final Clustering<T> clustering, final
    // ClusteringComparator comparator) {
    // this.clustering = clustering;
    // this.comparator = comparator;
    // }
    //
    // @Override
    // public Clustering<?> apply(ByteBuffer rowKey) {
    // CBuilder builder = CBuilder.create(comparator);
    // builder.add(rowKey);
    // for (int i = 0; i < clustering.size(); i++)
    // builder.add(clustering.get(i), clustering.accessor());
    //
    // // Note: if indexing a static column, prefix will be
    // // Clustering.STATIC_CLUSTERING
    // // so the Clustering obtained from builder::build will contain a value for
    // only
    // // the partition key. At query time though, this is all that's needed as the
    // // entire
    // // base table partition should be returned for any mathching index entry.
    // return builder.build();
    // }
    //
    // }
}
