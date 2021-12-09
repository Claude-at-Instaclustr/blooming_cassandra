package com.instaclustr.cassandra.bloom.idx.mem;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.function.IntConsumer;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.mem.tables.BufferTable;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BusyTable;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndexer;


/**
 * This is what Daniel called Bloofi2. Basically, instead of using a tree
 * structure like Bloofi (see BloomFilterIndex), we "transpose" the BitSets.
 *
 *
 * @author Daniel Lemire
 *
 * @param <E>
 */
public final class FlatBloofi implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BloomingIndexer.class);

    /*
     * each buffer entry accounts for 64 entries in the index. each there is one
     * long in each buffer entry for each bit in the bloom filter. each long is a
     * bit packed set of 64 flags, one for each entry.
     */
    private BusyTable busy;
    private BufferTable buffer;


    public FlatBloofi(File dataFile, File busyFile, int numberOfBits) throws IOException {

        buffer = new BufferTable( numberOfBits, dataFile );
        try {
            busy = new BusyTable( busyFile );
        } catch (IOException e) {
            try {
                buffer.close();
            } catch (IOException e2 ) {
                logger.error( "Error while closing buffer", e2);
            }
            throw e;
        }

    }

    @Override
    public void close() throws IOException {
        if (busy != null) {
            busy.close();
        }
        if (buffer != null) {
            buffer.close();
        }
    }


    public int add(ByteBuffer bloomFilter)  throws IOException {
        int idx = busy.newIndex();
        try {
            buffer.setBloomAt( idx, bloomFilter.asLongBuffer());
            return idx;
        } catch (IOException e ) {
            try {
                busy.clear( idx );
            }
            catch (IOException ignore) {
                logger.error( "Error when trying to clear index", e );
            }
            throw e;
        }
    }

    public void update( int idx, ByteBuffer bloomFilter ) throws IOException {
        buffer.setBloomAt( idx, bloomFilter.asLongBuffer());
    }

    private LongBuffer adjustBuffer( ByteBuffer bloomFilter ) {
        if (bloomFilter.remaining() == buffer.filterWords * Byte.SIZE)
        {
            return bloomFilter.asLongBuffer().asReadOnlyBuffer();
        }
        if (bloomFilter.remaining() > buffer.filterWords * Byte.SIZE) {
            throw new IllegalArgumentException( "Bloom filter is too long" );
        }
        // must be shorter
        byte[] buff = new byte[buffer.filterWords * Byte.SIZE];
        bloomFilter.get(buff, bloomFilter.position(), bloomFilter.remaining());
        return ByteBuffer.wrap(buff).asLongBuffer().asReadOnlyBuffer();
    }

    public void search(IntConsumer result, ByteBuffer bloomFilter) throws IOException {

        buffer.search( result, adjustBuffer( bloomFilter ), busy);
    }

    //    /**
    //     * Gets a packed index of entries that exactly match the filter.
    //     * @param filter the filter to match/
    //     * @return a packed index of entries.
    //     */
    //    private BitSet findExactMatch(BloomFilter filter) {
    //        long[] bits = BloomFilter.asBitMapArray(filter);
    //        long[] result = new long[buffer.size()];
    //        long[] busyBits = busy.toLongArray();
    //        /*
    //         * for each set of 64 filters in the index
    //         */
    //        for (int filterSetIdx = 0; filterSetIdx < result.length; filterSetIdx++) {
    //            /*
    //             * Each entry in the filter set is a map of 64 filters in the index to the bit
    //             * for the position. So filterSet[0] is a bit map of 64 index entries if the bit
    //             * is on in a specific position then that Bloom filter has bit 0 turned on.
    //             *
    //             */
    //            long[] filterSet = buffer.get(filterSetIdx);
    //
    //            /*
    //             * Build a list of the 64 filters in this chunk of index that have the proper
    //             * bits turned on.
    //             */
    //
    //            /*
    //             * remove is the map of all entries that we know do not match so remove all the
    //             * ones that are not in the busy list
    //             */
    //            long remove = ~busyBits[filterSetIdx];
    //
    //            // is the list of all entries that might match.
    //            long keep = ~0L;
    //            boolean foundKeep = false;
    //
    //            for (int idx = 0; idx < shape.getNumberOfBits(); idx++) {
    //                if (BitUtils.isSet(bits, idx)) {
    //                    foundKeep = true;
    //                    keep &= filterSet[idx];
    //                    if (keep == 0) {
    //                        // we are not keeping any so get out of the loop
    //                        break;
    //                    }
    //                } else {
    //                    remove |= filterSet[idx];
    //                    if (remove == ~0L) {
    //                        // we are removing them all so get out of the loop
    //                        break;
    //                    }
    //                }
    //            }
    //
    //            if (foundKeep) {
    //                result[filterSetIdx] = keep & ~remove;
    //            }
    //        }
    //
    //        return BitSet.valueOf(result);
    //    }

    public void delete(int idx) throws IOException {
        busy.clear(idx);
    }

    public int count() throws IOException {
        return busy.cardinality();
    }

}
