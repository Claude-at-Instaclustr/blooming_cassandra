package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;
import java.util.concurrent.Callable;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BusyTable extends BaseTable implements AutoCloseable {
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

    private class IndexScanner implements Callable<Boolean>, Iterator<Integer> {
        private final int maxBlock;
        private int block;
        private long mask;
        private long word;
        private long check;
        private int blockIdx;
        private Integer next;

        IndexScanner(int maxBlock) {
            this.maxBlock = maxBlock;
            this.block = 0;
            this.next = null;
        }

        private boolean matches() {
            return (check & mask) != 0;
        }

        public int getBlock() {
            return block;
        }

        @Override
        public Boolean call() {
            if (hasNext() && matches()) {
                writeBuffer.put(block, word | mask);
                return true;
            }
            return false;
        }

        private boolean findMatch() {
            while (block < maxBlock) {
                this.word = writeBuffer.get(block);
                this.check = word ^ ~0L; // convert all 0 to 1 and visa versa
                while (blockIdx < Long.SIZE) {
                    mask = BitMap.getLongBit(blockIdx);
                    if (matches()) {
                        return true;
                    }
                    blockIdx++;
                }
                block++;
            }
            return false;
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                if (findMatch()) {
                    next = Integer.valueOf((block * Long.SIZE) + blockIdx);
                }
            }
            return next != null;
        }

        @Override
        public Integer next() {
            try {
                return next;
            } finally {
                next = null;
            }

        }
    }

    /**
     * Returns an unused index value.
     * Will locate deleted indexes and use them first.
     * @return the index number.
     * @throws IOException on IO Error
     */
    public int newIndex() throws IOException {

        IndexScanner scanner = new IndexScanner(writeBuffer.limit());
        while (scanner.hasNext()) {
            try {
                if (sync(scanner, scanner.getBlock() * Long.BYTES, Long.BYTES, 4)) {
                    return scanner.next();
                }
            } catch (IOException e) {
                logger.warn("newIndex failure: {}", e.getMessage());
                // skip the entry
                scanner.next();
            }
        }
        // there is no old index so create a new one.
        try (RangeLock lock = getLock(extendBuffer(), Long.BYTES, 4)) {
            ByteBuffer buff = getWritableBuffer();
            writeBuffer = buff.asLongBuffer();
            buff.putLong(lock.getStart(), 1l);
            return lock.getStart() * Byte.SIZE;
        }
    }

    public void clear(int idx) throws IOException {
        int wordIdx = BitMap.getLongIndex(idx);
        sync(() -> writeBuffer.put(wordIdx, writeBuffer.get(wordIdx) & ~BitMap.getLongBit(idx)), wordIdx * Long.BYTES,
                Long.BYTES, 4);

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
