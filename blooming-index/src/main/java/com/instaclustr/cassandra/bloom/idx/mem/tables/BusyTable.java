package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.LongBuffer;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BusyTable extends AbstractTable implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BusyTable.class);

    private LongBuffer writeBuffer;

    public BusyTable(File busyFile) throws IOException {
        super(busyFile, Long.BYTES);
        writeBuffer = getWritableLongBuffer();
    }

    @Override
    public void close() throws IOException {
        writeBuffer = null;
        super.close();
    }

    @Override
    public String toString() {
        return "BusyTable: " + super.toString();
    }

    /**
     * Returns an unused index value.
     * Will locate deleted indexes and use them first.
     * @return the index number.
     * @throws IOException on IO Error
     */
    public int newIndex() throws IOException {

        int max = writeBuffer.limit();
        // find the first clear bit
        for (int count = 0; count < max; count++) {

            long word = writeBuffer.get(count);
            long check = word ^ ~0L; // convert all 0 to 1 and visa versa
            long mask;

            for (int idx = 0; idx < Long.SIZE; idx++) {
                mask = BitMap.getLongBit(idx);
                if ((check & mask) != 0) {
                    // create a supplier to verify bit is still open before
                    // overwriting it.
                    int block = count;
                    long msk = mask;
                    Supplier<Boolean> supplier = new Supplier<Boolean>() {

                        @Override
                        public Boolean get() {
                            long word = writeBuffer.get(block);
                            long check = word ^ ~0L; // convert all 0 to 1 and visa versa
                            if ((check & msk) != 0) {
                                writeBuffer.put(block, word | msk);
                                return true;
                            }
                            return false;
                        }
                    };

                    try {
                        if (sync(supplier, count*Long.BYTES, Long.BYTES, 4)) {
                            return idx;
                        }
                    } catch (SyncFailedException | TimeoutException e) {
                        logger.warn("newIndex failure: {}", e.getMessage());
                    }
                    // if we get here we just continue looking for the next open bit.
                }
            }
        }
        writeBuffer = extendBuffer().asLongBuffer();

        int idx = max * Long.SIZE;
        writeBuffer.put(idx, 1l);
        return idx;
    }

    public void clear(int idx) throws IOException {
        try {

            int wordIdx = BitMap.getLongIndex(idx);
            sync(() -> writeBuffer.put(wordIdx, writeBuffer.get(wordIdx) & ~BitMap.getLongBit(idx)),
                    wordIdx*Long.BYTES,
                    Long.BYTES,
                    4);

        } catch (TimeoutException e) {
            throw new IOException(e);
        }
    }

    public boolean isSet(int idx) throws IOException {
        LongBuffer buff = getLongBuffer();
        try {
            int wordIdx = BitMap.getLongIndex(idx);
            return (buff.get(wordIdx) & BitMap.getLongBit(idx)) > 0;
        } catch (IndexOutOfBoundsException exception) {
            // looked beyond buffer so not set.
            return false;
        }
    }

    public int cardinality() throws IOException {
        LongBuffer buff = getLongBuffer();
        int result = 0;
        while (buff.hasRemaining()) {
            result += Long.bitCount(buff.get());
        }
        return result;
    }
}
