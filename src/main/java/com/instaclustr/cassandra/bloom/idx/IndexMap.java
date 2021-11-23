package com.instaclustr.cassandra.bloom.idx;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import com.instaclustr.cassandra.bloom.idx.std.BFUtils;

/**
 * A Multidimensional Bloom filter entry key.
 */
public class IndexMap {
    /**
     * The byte position in the bloom filter for this code
     */
    private int position;
    /**
     * The code from the position
     */
    private int[] codes;

    /**
     * The number of bytes the data for the key uses.
     */
    public static final int BYTES = Integer.BYTES+1;

    /**
     * Creates an IndexMap from the IndexKey.
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
     * @param position the byte postion of the code in the bloom filter.
     * @param code the code from the filter.
     */
    public IndexMap(int position, int[] codes ) {
        this.position=position;
        this.codes=codes;
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
    public int[] getCode() {
        return codes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder( String.format( "IndexMap[%d, [", position, hashCode() ));
        for (int i=0;i<codes.length;i++) {
            sb.append( String.format( "%s %2x",  i>0? ",":"", codes[i]));
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
