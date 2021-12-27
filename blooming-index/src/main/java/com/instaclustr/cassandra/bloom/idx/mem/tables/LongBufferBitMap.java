package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.nio.LongBuffer;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

import org.apache.commons.collections4.bloomfilter.BitMap;

/**
 * Creates a bitmap class for the LongBuffer.
 *
 */
public class LongBufferBitMap {
    private final LongBuffer buffer;

    /**
     * Constructs a buffer.
     * Modifications made to this buffer will be applied the the buffer parameter.
     * @param buffer the buffer to modify/query.
     */
    public LongBufferBitMap(LongBuffer buffer) {
        this.buffer = buffer.duplicate();
    }

    /**
     * Gets an iterator over the indices of the on bits in this buffer.
     * @param limit All returned values will be less than the limit.
     * @return A primitive iterator OfInt that contains the indices of the active bits less than the limit.
     */
    public PrimitiveIterator.OfInt indices(int limit) {
        return new LongBufferIteratorOfInt(limit);
    }

    /**
     * Defines the LongLogical functional interface.
     */
    @FunctionalInterface
    private interface LongLogical {
        long exec(long a, long b);
    }

    /**
     * Private method to apply a LongLogical to every long in the two buffers.
     * @param other the other buffer
     * @param fn the function to apply.
     */
    private void func(LongBuffer other, LongLogical fn) {
        if (other.remaining() != buffer.remaining()) {
            throw new IllegalArgumentException("Buffer wrong size");
        }
        LongBuffer inBuffer = other.duplicate();
        LongBuffer outBuffer = buffer.duplicate();
        while (inBuffer.hasRemaining()) {
            outBuffer.put(fn.exec(outBuffer.get(outBuffer.position()), inBuffer.get()));
        }
    }

    /**
     * Performs a logical OR between this buffer and the other buffer.
     * Results are stored in this buffer.
     * @param other the Other buffer.
     */
    public void or(LongBuffer other) {
        func(other, (a, b) -> (a | b));
    }

    /**
     * Performs a logical AND between this buffer and the other buffer.
     * Results are stored in this buffer.
     * @param other the Other buffer.
     */
    public void and(LongBuffer other) {
        func(other, (a, b) -> (a & b));
    }

    /**
     * Performs a logical XOR between this buffer and the other buffer.
     * Results are stored in this buffer.
     * @param other the Other buffer.
     */
    public void xor(LongBuffer other) {
        func(other, (a, b) -> (a ^ b));
    }

    /**
     * Checks if a bit is set.
     * @param idx the index to check.
     * @return true if the bit was set, false otherwise.
     * @Throws IndexOutOfBoundsException exception if idx is negative.
     */
    public boolean isSet(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException(String.format("idx (%s) may not be less than zero (0)", idx));
        }
        try {
            long mask = BitMap.getLongBit(idx);
            return (buffer.get(BitMap.getLongIndex(idx)) & mask) == mask;
        } catch (IndexOutOfBoundsException e) {
            return false;
        }
    }

    /**
     * An iterator of int over the LongBuffer.
     * returns the index of the enabled bits for the LongBuffer.
     */
    private class LongBufferIteratorOfInt implements PrimitiveIterator.OfInt {
        /**
         * The next index.
         */
        private int next = -1;
        /**
         * The last index.
         */
        private int last = -1;
        /**
         * The maximum index.
         */
        private final int limit;

        /**
         * Constructor.
         * @param limit the maximum index to process.
         */
        LongBufferIteratorOfInt(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            if (next < 0) {
                int idxStart = (last < 0 ? 0 : last + 1) % Long.SIZE;
                int max = buffer.remaining();
                for (int count = BitMap.getLongIndex(last < 0 ? 0 : last + 1); count < max; count++) {
                    long word = buffer.get(count);
                    long mask;
                    for (int idx = idxStart; idx < Long.SIZE; idx++) {
                        mask = BitMap.getLongBit(idx);
                        if ((word & mask) != 0) {
                            next = idx + (count * Long.SIZE);
                            return next < limit;
                        }
                    }
                    idxStart = 0;
                }
                return false;
            }
            return next < limit;
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

}
