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
package com.instaclustr.cassandra.bloom.idx;

import java.nio.ByteBuffer;

/**
 * A Multidimensional Bloom filter entry key.
 * <p>Keys are naturally ordered by their selectivity
 * and position, with the highest selectivity first.</p>
 */
public class IndexKey implements Comparable<IndexKey> {

    /**
     * A list of bytes to matching bytes in the bloom filter.
     */
    private static final int[][] byteTable;
    /**
     * the selectivity for each byte.
     */
    private static final int[] selectivityTable;

    static {
        // populate the byteTable annd selectivity tables.
        int limit = (1 << Byte.SIZE);
        byteTable = new int[limit][];
        selectivityTable = new int[limit];
        int[] buffer;
        int count = 0;

        for (int i = 1; i < limit; i++) {
            count = 0;
            buffer = new int[256];
            for (int j = 1; j < limit; j++) {
                if ((j & i) == i) {
                    buffer[count++] = j;
                    selectivityTable[j]++;
                }
            }
            byteTable[i] = new int[count];
            System.arraycopy(buffer, 0, byteTable[i], 0, count);
        }

    }

    /**
     * The byte position in the bloom filter for this code
     */
    private int position;
    /**
     * The code from the position
     */
    private int code;

    /**
     * The number of bytes the data for the key uses.
     */
    public static final int BYTES = Integer.BYTES * 2;

    /**
     * Constructor.
     * @param position the byte postion of the code in the bloom filter.
     * @param code the code from the filter.
     */
    public IndexKey(int position, int code) {
        this.position = position;
        this.code = 0xFF & code;
    }

    /**
     * Gets the position of the code for this key.
     * @return the position of the code in the bloom filter.
     */
    public int getPosition() {
        return position;
    }

    /**
     * Gets the code for this key.
     * @return the code from the bloom filter at the position.
     */
    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return String.format("IndexKey[%d, 0x%02x]", position, code);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IndexKey) {
            IndexKey other = (IndexKey) o;
            return code == other.getCode() && position == other.getPosition();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getCode();
    }

    /**
     * Returns the IndexKey as a ByteBuffer suitable for index writing
     * @return ByteBuffer for key.
     */
    public ByteBuffer asKey() {
        ByteBuffer result = ByteBuffer.allocate(BYTES);
        result.putInt(getPosition());
        result.putInt(getCode());
        result.flip();
        return result;
    }

    /**
     * Converts this IndexKey into an IndexMap
     * @return the IndexMap for this key.
     * @see IndexMap#IndexMap(IndexKey)
     */
    public IndexMap asMap() {
        return new IndexMap(getPosition(), byteTable[getCode()]);
    }

    /**
     * Returns {@code true} if the code is zero.
     * @return {@code true} if the code is zero.
     */
    public boolean isZero() {
        return getCode() == 0;
    }

    @Override
    public int compareTo(IndexKey other) {
        int i = Integer.compare(selectivityTable[other.getCode()], selectivityTable[getCode()]);
        return i == 0 ? Integer.compare(this.position, other.position) : i;
    }
}
