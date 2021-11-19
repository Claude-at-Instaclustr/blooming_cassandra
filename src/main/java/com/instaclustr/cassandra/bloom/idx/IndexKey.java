package com.instaclustr.cassandra.bloom.idx;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Predicate;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import com.instaclustr.cassandra.bloom.idx.std.BFUtils;

/**
 * A Multidimensional Bloom filter entry key.
 */
public class IndexKey implements Comparable<IndexKey> {
    /**
     * The byte position in the bloom filter for this code
     */
    private int position;
    /**
     * The code from the position
     */
    private byte code;

    /**
     * The number of bytes the data for the key uses.
     */
    public static final int BYTES = Integer.BYTES+1;

    /**
     * Constructor.
     * @param position the byte postion of the code in the bloom filter.
     * @param code the code from the filter.
     */
    public IndexKey(int position, byte code ) {
        this.position=position;
        this.code=code;
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
    public byte getCode() {
        return code;
    }


    public interface Consumer extends java.util.function.Consumer<IndexKey> {

    }

    @Override
    public boolean equals( Object o) {
        return o instanceof IndexKey? compareTo( (IndexKey) o ) == 0 : false;
    }

    @Override
    public int hashCode() {
        return BFUtils.selectivityTable[getCode()];
    }

    public ByteBuffer asKey() {
        ByteBuffer result = ByteBuffer.allocate(BYTES);
        result.putInt( getPosition() );
        result.put( getCode() );
        return result;
    }

    public IndexMap asMap() {
        return new IndexMap(this);
    }

    public boolean isZero() {
        return getCode()==0;
    }

    @Override
    public int compareTo(IndexKey other) {
        int i = Integer.compare( other.hashCode(), hashCode());
        return i==0? Integer.compare( this.position, other.position) : i;
    }
}
