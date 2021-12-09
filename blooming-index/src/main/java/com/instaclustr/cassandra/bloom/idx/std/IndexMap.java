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

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.instaclustr.iterator.util.ExtendedIterator;
import com.instaclustr.iterator.util.WrappedIterator;

/**
 * A Multidimensional Bloom filter entry map.
 */
public class IndexMap {
    /**
     * A list of bytes to matching bytes in the bloom filter.
     */
    private static final int[][] byteTable;

    static {
        // populate the byteTable annd selectivity tables.
        int limit = (1 << Byte.SIZE);
        byteTable = new int[limit][];
        int[] buffer;
        int count = 0;

        for (int i = 1; i < limit; i++) {
            count = 0;
            buffer = new int[256];
            for (int j = 1; j < limit; j++) {
                if ((j & i) == i) {
                    buffer[count++] = j;
                }
            }
            byteTable[i] = new int[count];
            System.arraycopy(buffer, 0, byteTable[i], 0, count);
        }
    }

    private IndexKey key;

    /**
     * Constructor.
     * @param position the byte position of the code in the bloom filter.
     * @param codes that match the code from the filter.
     */
    public IndexMap(IndexKey key) {
        this.key = key;
    }

    /**
     * Gets the position of the codes for this map.
     * @return the position of the codes in the bloom filter.
     */
    public int getPosition() {
        return key.getPosition();
    }

    /**
     * Gets the codes for this key.
     * @return the codes that match the bloom filter at the position.
     */
    public int[] getCodes() {
        return byteTable[key.getCode()];
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(String.format("IndexMap[%d, [", getPosition()));
        int[] codes = getCodes();
        for (int i = 0; i < codes.length; i++) {
            sb.append(String.format("%s 0x%02x", i > 0 ? "," : "", codes[i]));
        }
        return sb.append(" ]]").toString();
    }

    /**
     * Returns the codes in the map as IndexKeys.
     * @return an ExtendedIterator of IndexKeys
     */
    public ExtendedIterator<IndexKey> getKeys() {
        return WrappedIterator.create(new Iterator<IndexKey>() {
            int idx = 0;

            @Override
            public boolean hasNext() {
                return idx < getCodes().length;
            }

            @Override
            public IndexKey next() {
                if (hasNext()) {
                    return new IndexKey(getPosition(), getCodes()[idx++]);
                }
                throw new NoSuchElementException();
            }

        });
    }

}
