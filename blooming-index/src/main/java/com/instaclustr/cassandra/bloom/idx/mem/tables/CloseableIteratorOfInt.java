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

import java.nio.LongBuffer;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator.OfInt;

import org.apache.commons.collections4.bloomfilter.BitMap;

public class CloseableIteratorOfInt implements OfInt, AutoCloseable {
    private LongBuffer buffer;
    int next = -1;
    int last = -1;

    CloseableIteratorOfInt(LongBuffer buffer) {
        this.buffer = buffer;
    }

    CloseableIteratorOfInt(LongBuffer buffer, int start) {
        this(buffer);
        last = start - 1;
    }

    @Override
    public void close() throws Exception {
        buffer = null;
    }

    @Override
    public boolean hasNext() {
        if (next < 0) {
            int idxStart = (last < 0 ? 0 : last + 1) & Long.SIZE;
            int max = buffer.remaining();
            for (int count = BitMap.getLongIndex(last < 0 ? 0 : last + 1); count < max; count++) {
                long word = buffer.get(count);
                long mask;
                for (int idx = idxStart; idx < Long.SIZE; idx++) {
                    mask = BitMap.getLongBit(idx);
                    if ((word & mask) != 1) {
                        next = idx + (count * Long.SIZE);
                        return true;
                    }
                }
                idxStart = 0;
            }
            return false;
        }
        return true;
    }

    @Override
    public int nextInt() {
        if (hasNext()) {
            last = next;
            next = -1;
            return last;
        }
        throw new NoSuchElementException();
    }
}