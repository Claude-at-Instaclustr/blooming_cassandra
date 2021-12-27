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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.IntPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a file contains status information for a Buffer table.
 *
 */
public class BufferTableIdx extends BaseTable implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BufferTableIdx.class);

    /**
     * Convert a bit position ot a byte map for that position
     * @param bit the bit position to map.
     * @return a byte that is a bit mask to extract the position bit.
     */
    private static byte mkMap(int bit) {
        return (byte) (1 << bit);
    }

    /**
     * A sorted set of deleted entries to quicky find deleted entries with enough space for a buffer.
     */
    private ConcurrentSkipListSet<Entry> deletedEntries = new ConcurrentSkipListSet<>();

    /**
     * The byte buffer we are reading/writeing.
     */
    private volatile ByteBuffer buffer;

    /**
     * Flag for a deleted entry (available for reuse)
     */
    private static byte DELETED_FLG = mkMap(0);

    /**
     * Flagfor Uninitialized entries.
     */
    private static byte UNINITIALIZED_FLG = mkMap(1);
    /**
     * Flag for an invalid entry.  Was once an entry but the allocation was
     * merged with another entry or is otherwise not available.
     */
    private static byte INVALID_FLG = (byte) (DELETED_FLG | UNINITIALIZED_FLG);

    // the first byte is the Flag byte
    /**
     * The position in the block of the offset value
     */
    private static int OFFSET_BYTE = 1;
    /**
     * The position in the block of the length value
     */
    private static int LEN_BYTE = OFFSET_BYTE + Integer.BYTES;
    /**
     * The position in the block of the allocation value
     */
    private static int ALLOC_BYTE = LEN_BYTE + Integer.BYTES;
    /**
     * The length of a block.
     */
    private static int BLOCK_SIZE = ALLOC_BYTE + Integer.BYTES;

    /**
     * An entry in the Buffer Table Index.
     */
    public abstract class Entry implements Comparable<Entry> {

        /**
         * the offset of this entry in the table.
         */
        private int offset;

        /**
         * Constructor.
         * @param offset the offset of the entry in the table.
         */
        protected Entry(int offset) {
            this.offset = offset;
        }

        /**
         * Gets the offset of the block in the Buffer Table.
         * @return the offset of the block in the Buffer table.
         */
        public final int getBlockOffset() {
            return offset;
        }

        /**
         * Gets the block number in the Buffer table for this entry.
         * @return the block number for this entry.
         */
        public final int getBlock() {
            return getBlockOffset() / BLOCK_SIZE;
        }

        /**
         * Gets the allocations size of the block in the Buffer Table.
         * @return the allocation size of the block in the Buffer table.
         */
        public abstract int getAlloc();

        /**
         * Compare Entries by allocation and then offset.
         */
        @Override
        public final int compareTo(Entry arg0) {
            int result = Integer.compare(getAlloc(), arg0.getAlloc());
            if (result == 0) {
                result = Integer.compare(getBlockOffset(), arg0.getBlockOffset());
            }
            return result;
        }

        @Override
        public final boolean equals(Object obj) {
            return (obj instanceof Entry) ? compareTo((Entry) obj) == 0 : false;
        }

        @Override
        public final int hashCode() {
            return getAlloc();
        }

        @Override
        public final String toString() {
            return String.format("%s[ b:%s o:%s ]", this.getClass().getSimpleName(), getBlock(), getBlockOffset());
        }

    }

    /**
     * An entry in the Buffer Table Idx.
     */
    class IdxEntry extends Entry {

        /**
         * Constructor.
         * @param block the block for this entry.
         * @throws IOException on IO Error.
         */
        IdxEntry(int block) throws IOException {
            super(block * BLOCK_SIZE);
            if (!checkBlockAlignment(getFileSize(), BLOCK_SIZE)) {
                throw new IllegalStateException("Blocks are not aligned with file");
            }
        }

        /**
         * Sets an integer value within the block.
         * @param blockOffset the offset of the integer within the block.
         * @param value the value to set.
         * @throws IOException on IO Error
         */
        private void doPut(int blockOffset, int value) throws IOException {
            final ByteBuffer writeBuffer = getWritableBuffer();
            final int startByte = getBlockOffset() + blockOffset;
            sync(() -> writeBuffer.putInt(startByte, value), startByte, Integer.BYTES, 4);
        }

        /**
         * Sets flag values
         * @param flag the flag(s) to set
         * @param state The state to set the flag(s)
         * @throws IOException on IO Error
         */
        private void setFlg(byte flag, boolean state) throws IOException {
            final ByteBuffer writeBuffer = getWritableBuffer();
            final int startByte = getBlockOffset();
            boolean wasAvailable = isAvailable();
            if (state) {
                sync(() -> writeBuffer.put(startByte, (byte) (writeBuffer.get(startByte) | flag)), startByte, 1, 4);
            } else {
                sync(() -> writeBuffer.put(startByte, (byte) (writeBuffer.get(startByte) & ~flag)), startByte, 1, 4);
            }
            if (wasAvailable != isAvailable()) {
                if (wasAvailable) {
                    deletedEntries.remove(this);
                } else {
                    deletedEntries.add(this);
                }
            }
        }

        /**
         * Get a Range lock for this Entry.
         * @param retryCount the number of retry attempts
         * @return the RangeBlock.
         * @throws OutputTimeoutException if lock could not be established.
         */
        public RangeLock lock(int retryCount) throws OutputTimeoutException {
            return getLock(getBlockOffset(), BLOCK_SIZE, retryCount);
        }

        /**
         * Checks the state of a flag.
         * The predicate should expect to receive the entire flag byte.
         * @param predicate the Predicate that performs the check.
         * @return true the state returned by the predicate.
         */
        private boolean checkFlag(IntPredicate predicate) {
            return predicate.test(buffer.get(getBlockOffset()));
        }

        /**
         * Checks the deleted flag
         * @return {@code true} if the deleted flag is set.
         */
        public boolean isDeleted() {
            return checkFlag((b) -> (b & DELETED_FLG) == DELETED_FLG);
        }

        /**
         * Sets the deleted flag state.
         * @param state state to put the flag into.
         * @throws IOException on IO error.
         */
        public void setDeleted(boolean state) throws IOException {
            setFlg(DELETED_FLG, state);
        }

        /**
         * Cheks the invalid state.  Entries are invalid if they are deleted and uninitialized.
         * @return {@code true} if the entry is invalid.
         */
        public boolean isInvalid() {
            return checkFlag((b) -> (b & INVALID_FLG) == INVALID_FLG);
        }

        /**
         * Sets the invalid state.
         * This will set both the DELETED and UNINITIALIZED to the specified state.
         * @param state the state to set.
         * @throws IOException on IO Error.
         */
        public void setInvalid(boolean state) throws IOException {
            setFlg(INVALID_FLG, state);
        }

        /**
         * Determines if the entry is available.
         * Entries are available if they are deleted and not invalid.
         * @return {@code true} if the entry is available.
         */
        public boolean isAvailable() {
            return checkFlag((b) -> (b & INVALID_FLG) == DELETED_FLG);
        }

        /**
         * Sets the initialized flag.
         * @param state the state to set the initialized flag to.
         * @throws IOException on IO Error
         */
        public void setInitialized(boolean state) throws IOException {
            setFlg(UNINITIALIZED_FLG, !state);
        }

        /**
         * Checks the initialization state of the entry
         * @return {@code true} if the entry is initialized, {@code false} otherwise.
         */
        public boolean isInitialized() {
            return !checkFlag((b) -> (b & UNINITIALIZED_FLG) == UNINITIALIZED_FLG);
        }

        /**
         * Gets the offset in the data table this index points to..
         * @return the offset in the data table for the buffer this index is for.
         */
        public int getOffset() {
            return buffer.getInt(getBlockOffset() + OFFSET_BYTE);
        }

        /**
         * Sets the offset in the data table this index points to
         * @param offset the offset.
         * @throws IOException on IO error
         */
        void setOffset(int offset) throws IOException {
            doPut(OFFSET_BYTE, offset);
        }

        /**
         * Gets the length of the data table entry that is filled by data.
         * @return the length of the data table entry.
         */
        public int getLen() {
            return buffer.getInt(getBlockOffset() + LEN_BYTE);
        }

        /**
         * Sets the length of the data table entry that is filled by data.
         * @param len the length of the entry.
         * @throws IOException on IO error.
         */
        void setLen(int len) throws IOException {
            doPut(LEN_BYTE, len);
        }

        /**
         * Gets the allocated space in the data table for this entry.
         * @return the allocated space in the data table for this entry.
         */
        @Override
        public int getAlloc() {
            return buffer.getInt(getBlockOffset() + ALLOC_BYTE);
        }

        /**
         * Sets the allocated space in the data table for this entry.
         * @param alloc the allocated space in the data table for this entry.
         * @throws IOException on IO error.
         */
        void setAlloc(int alloc) throws IOException {
            doPut(ALLOC_BYTE, alloc);
        }

        /**
         * Locks this entry for update.
         * The RangeLock must be released by a call to {@code close} or {@code release}
         * @return The range lock for this entry.
         * @throws OutputTimeoutException if the lock could not be achieved.
         */
        public RangeLock lock() throws OutputTimeoutException {
            return getLock(getBlockOffset(), BLOCK_SIZE, 4);
        }
    }

    /**
     * An Entry implementaiton used for searching the deleted entries
     *
     */
    class SearchEntry extends Entry {

        /**
         * The minimum allocation to search for.
         */
        private int alloc;

        /**
         * Constructor.
         * @param alloc the minimum allocation size to search for.
         */
        SearchEntry(int alloc) {
            super(-1);
            this.alloc = alloc;
        }

        @Override
        public int getAlloc() {
            return alloc;
        }
    }

    /**
     * Constructor
     * @param bufferFile the file to read/write.
     * @throws IOException on IO error.
     */
    public BufferTableIdx(File bufferFile) throws IOException {
        this(bufferFile, BaseTable.READ_WRITE);
    }

    /**
     * Constructor
     * @param bufferFile the file to operate on.
     * @param readOnly i f{@code true} the file will be opened in read only mode, otherwise it will be opened in read/write mode.
     * @throws IOException on IO error.
     */
    public BufferTableIdx(File bufferFile, boolean readOnly) throws IOException {
        super(bufferFile, BLOCK_SIZE, readOnly);
        this.buffer = getBuffer();
        registerExtendNotification(() -> buffer = getBuffer());
        executor.submit(() -> buildDeletedSet());
    }

    /**
     * Method to scan the entries locating the deleted entries and placing them into the deletedEntries collection.
     */
    private void buildDeletedSet() {
        try {
            for (int i = 0; i < getFileSize() / getBlockSize(); i++) {
                try {
                    IdxEntry entry = new IdxEntry(i);
                    if (entry.isAvailable()) {
                        deletedEntries.add(entry);
                    }
                } catch (Exception e) {
                    logger.error("Error building deleted set at block " + i);
                }
            }
        } catch (Exception e) {
            logger.error("Error building deleted set", e);
        }
    }

    /**
     * Gets the entry for the block from the file.
     * @param block the block number to retrieve.
     * @return an IdxEntry for the block.
     * @throws IOException on IO Error.
     */
    public IdxEntry get(int block) throws IOException {
        ensureBlock(block + 1);
        return new IdxEntry(block);
    }

    @Override
    public void close() throws IOException {
        super.close();
        deletedEntries = null;
    }

    /**
     * Searches for an empty block of specified length.
     * If an empty block is found is is marked as not deleted and not initialized.
     * @param length the minimum length of the block.
     * @return the IdxEntry that will accept the length or {@code null} if none found.
     * @throws IOException on IO error
     */
    public IdxEntry search(int length) throws IOException {
        if (!deletedEntries.isEmpty()) {
            SearchEntry entry = new SearchEntry(length);
            IdxEntry idx = (IdxEntry) deletedEntries.higher(entry);
            logger.debug("Search located entry {} {}", idx.getBlock(), idx.getAlloc());
            boolean good = false;

            try {
                good = sync(() -> {
                    if (idx.isAvailable()) {
                        idx.setDeleted(false);
                        idx.setInitialized(false);
                        return true;
                    } else {
                        return false;
                    }
                }, idx.getBlockOffset(), BLOCK_SIZE, 0);
                if (good) {
                    return idx;
                }
            } catch (IOException e) {
                // fall through
            }
        }
        return null;
    }

    /**
     * Adds a new block to the index.
     * The new block is uninitialized.
     * The alloc and len values are both set to the len.
     * @param offset the offset on the data table for the buffer.
     * @param len the allocated length in the data table for the buffer.
     * @return the IdxEntry for the new block.
     * @throws IOException on IO Error.
     */
    public IdxEntry addBlock(int offset, int len) throws IOException {
        try (RangeLock lock = getLock(extendBuffer(1), BLOCK_SIZE, 4)) {
            IdxEntry idxEntry = new IdxEntry(lock.getStart() / BLOCK_SIZE);
            idxEntry.setLen(len);
            idxEntry.setAlloc(len);
            idxEntry.setOffset(offset);
            return idxEntry;
        }
    }

    /**
     * Deletes a block from the index.
     * @param block the block to delete.
     * @throws IOException on IO Error.
     */
    public void deleteBlock(int block) throws IOException {
        IdxEntry idxEntry = new IdxEntry(block);
        idxEntry.setDeleted(true);
        deletedEntries.add(idxEntry);
    }
}
