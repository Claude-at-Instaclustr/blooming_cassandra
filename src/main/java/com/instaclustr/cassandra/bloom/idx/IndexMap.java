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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import com.instaclustr.cassandra.bloom.idx.std.BFUtils;

/**
 * A Multidimensional Bloom filter entry map.
 */
public class IndexMap {
    /**
     * The byte position in the bloom filter for this map
     */
    private int position;
    /**
     * All of the matching codes for the position.
     */
    private int[] codes;

    /**
     * Creates an IndexMap from an IndexKey.
     * <p>The map is constructed by
     * retrieving the BFUtils.byteTable entries for {@code key.getCode()}.
     * Resulting map has the same position as the original key.</p>
     * @param key the Key to create the map from.
     */
    public IndexMap( IndexKey key ) {
        this( key.getPosition(), BFUtils.byteTable[key.getCode()]);
    }

    /**
     * Constructor.
     * @param position the byte position of the code in the bloom filter.
     * @param codes that match the code from the filter.
     */
    public IndexMap(int position, int[] codes ) {
        this.position=position;
        this.codes=codes;
    }

    /**
     * Gets the position of the codes for this map.
     * @return the position of the codes in the bloom filter.
     */
    public int getPosition() {
        return position;
    }

    /**
     * Gets the codes for this key.
     * @return the codes that match the bloom filter at the position.
     */
    public int[] getCode() {
        return codes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder( String.format( "IndexMap[%d, [", position, hashCode() ));
        for (int i=0;i<codes.length;i++) {
            sb.append( String.format( "%s 0x%02x",  i>0? ",":"", codes[i]));
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
                return idx<codes.length;
            }

            @Override
            public IndexKey next() {
                if (hasNext()) {
                    return new IndexKey( getPosition(), codes[idx++]);
                }
                throw new NoSuchElementException();
            }

        });
    }

}
