package com.instaclustr.cassandra.bloom.idx.std;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import com.instaclustr.cassandra.bloom.idx.IndexKey;

public class BFUtils {

    private BFUtils() {
    }
    /**
     * A list of bytes to matching bytes in the bloom filter.
     */
    public static final int[][] byteTable;
    public static final int[] selectivityTable;

    static {
        // populate the byteTable
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
                    buffer[count++] =  j;
                    selectivityTable[j]++;
                }
            }
            byteTable[i] = new int[count];
            System.arraycopy(buffer, 0, byteTable[i],0,count);
        }

    }

    public static byte[] extractCodes( ByteBuffer bloomFilter ) {
        LongBuffer buff = bloomFilter == null? LongBuffer.allocate(0) : bloomFilter.asLongBuffer();
        byte[] codes = new byte[buff.remaining()*Long.BYTES];
        int pos=0;
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

    public static ExtendedIterator<IndexKey> getIndexKeys( ByteBuffer bloomFilter ) {
        return getIndexKeys( extractCodes( bloomFilter ));
    }

    public static ExtendedIterator<IndexKey> getIndexKeys( final byte[] codes ) {
        return WrappedIterator.create( new Iterator<IndexKey>(){
            int i=0;

            @Override
            public boolean hasNext() {
                return i<codes.length;
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
        } ).filterDrop( IndexKey::isZero );
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

    public static ExtendedIterator<ByteBuffer> getIndexedValues(ByteBuffer partitionKey,
            Clustering<?> clustering,
            Iterator<IndexKey> keys) {

        ByteBuffer clusterKeys[] = clustering.getBufferArray();
        int cap = IndexKey.BYTES+partitionKey.capacity();
        for (ByteBuffer b  : clusterKeys) {
            cap += b.capacity();
        }
        final int capacity = cap;

        return WrappedIterator.create( keys )
                .mapWith( key -> {
                    ByteBuffer result = ByteBuffer.allocate(capacity);
                    result.putInt( key.getPosition() );
                    result.putInt( key.getCode() );
                    result.put(partitionKey);
                    for (ByteBuffer b : clusterKeys) {
                        result.put( b );
                    }
                    return result;
                });
    }


    static class IndexClusteringBuilder<T> implements Function<ByteBuffer,Clustering<?>> {
        private final Clustering<T> clustering;
        private final ClusteringComparator comparator;

        public IndexClusteringBuilder( final Clustering<T> clustering, final ClusteringComparator comparator)
        {
            this.clustering = clustering;
            this.comparator = comparator;
        }

        @Override
        public Clustering<?> apply(ByteBuffer rowKey) {
            CBuilder builder = CBuilder.create(comparator);
            builder.add(rowKey);
            for (int i = 0; i < clustering.size(); i++)
                builder.add(clustering.get(i), clustering.accessor());

            // Note: if indexing a static column, prefix will be Clustering.STATIC_CLUSTERING
            // so the Clustering obtained from builder::build will contain a value for only
            // the partition key. At query time though, this is all that's needed as the entire
            // base table partition should be returned for any mathching index entry.
            return builder.build();
        }

    }
}
