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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.function.IntPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a file that contains the offset of the index into a key file.
 *
 */
public class BufferTableIdx extends BaseTable implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BufferTableIdx.class);
    /*
     * Structure of the keytable index
     *
     * byte flags int offset int len int alloc
     *
     */

    private static byte mkMap(int bit) {
        return (byte) (1 << bit);
    }

    /**
     * Flag for a deleted entry (available for reuse)
     */
    private static byte DELETED_FLG = mkMap(0);

    private static byte UNINITIALIZED_FLG = mkMap(1);
    /**
     * Flag for an invalid entry.  Was once an entry but the allocation was
     * merged with another entry or is otherwise not available.
     */
    private static byte INVALID_FLG = (byte) (DELETED_FLG | UNINITIALIZED_FLG);

    private static int FLAG_BYTE = 0;
    private static int OFFSET_BYTE = 1;
    private static int LEN_BYTE = OFFSET_BYTE + Integer.BYTES;
    private static int ALLOC_BYTE = LEN_BYTE + Integer.BYTES;
    private static int BLOCK_SIZE = ALLOC_BYTE + Integer.BYTES;

    /**
     * Package private so that other classes in this package can use it.
     *
     */
    class IdxEntry {
        private final ByteBuffer buffer;
        // /**
        // * The block number for this entry
        // */
        // private int block;
        /**
         * The byte offset into the file for this entry.
         */
        private int offset;

        IdxEntry(ByteBuffer buffer, int block) {
            this.buffer = buffer;
            this.offset = block * BLOCK_SIZE;
        }

        @Override
        public String toString() {
            return String.format("BufferTableIdx[ b:%s o:%s ]", getBlock(), offset);
        }

        /**
         * Gets the table block for this entry.
         * @return the table block for this entry.
         */
        public int getBlock() {
            return this.offset / BLOCK_SIZE;
        }

        /**
         * Sets an integer value within the block.
         * @param blockOffset the offset of the integer within the block.
         * @param value the value to set.
         * @throws IOException on IO Error
         */
        private void doPut(int blockOffset, int value) throws IOException {
            final ByteBuffer writeBuffer = getWritableBuffer();
            final int startByte = offset + blockOffset;
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
            final int startByte = offset + FLAG_BYTE;
            if (state) {
                sync(() -> writeBuffer.put(startByte, (byte) (writeBuffer.get(startByte) | flag)), startByte, 1, 4);
            } else {
                sync(() -> writeBuffer.put(startByte, (byte) (writeBuffer.get(startByte) & ~flag)), startByte, 1, 4);
            }
        }

        /**
         * Checks the state of a flag.
         * The predicate should expect to receive the entire flag byte.
         * @param predicate the Predicate that performs the check.
         * @return true the state returned by the predicate.
         */
        private boolean checkFlag(IntPredicate predicate) {
            return predicate.test(buffer.get(offset + FLAG_BYTE));
        }

        /**
         * Checks the deleted flag
         * @return true if the deleted flag is set.
         */
        public boolean isDeleted() {
            return checkFlag((b) -> (b & DELETED_FLG) > 0);
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
         * @return true if the entry is invalid.
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
         * @return true if the entry is available.
         */
        public boolean isAvailable() {
            return checkFlag((b) -> (b & INVALID_FLG) == DELETED_FLG);
        }

        /**
         * Sets the initialized flag.
         * @param state the state to set the initialzed flag to.
         * @throws IOException on IO Error
         */
        public void setInitialized(boolean state) throws IOException {
            setFlg(UNINITIALIZED_FLG, !state);
        }

        /**
         * Checks the initialization state of the entry
         * @return true if the entry is initialized, false otherwise.
         */
        public boolean isInitialized() {
            return !checkFlag((b) -> (b & UNINITIALIZED_FLG) > 0);
        }

        /**
         * Gets the offset in the data table this index points to..
         * @return the offset in the data table for the buffer this index is for.
         */
        int getOffset() {
            return buffer.getInt(offset + OFFSET_BYTE);
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
        int getLen() {
            return buffer.getInt(offset + LEN_BYTE);
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
         * @return the allcoated space in the data table for this entry.
         */
        int getAlloc() {
            return buffer.getInt(offset + ALLOC_BYTE);
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
            return getLock(offset, BLOCK_SIZE, 4);
        }
    }

    /**
     * Constructor
     * @param bufferFile the file to operate on.
     * @throws IOException on IO error.
     */
    public BufferTableIdx(File bufferFile) throws IOException {
        super(bufferFile, BLOCK_SIZE);

    }

    @Override
    public String toString() {
        return "KeyTableIdx: " + super.toString();
    }

    /**
     * Gets the entry for the block from the file.
     * @param block the block number to retrieve.
     * @return an IdxEntry for the block.
     * @throws IOException on IO Error.
     */
    public IdxEntry get(int block) throws IOException {
        ensureBlock(block);
        return new IdxEntry(getBuffer(), block);
    }

    class Scanner implements Callable<Boolean>, Iterator<IdxEntry> {

        private final ByteBuffer buffer;
        private final int length;
        private IdxEntry next;
        private int lastEntryBlock;

        Scanner(ByteBuffer buffer, int length) {
            this.buffer = buffer;
            this.length = length;
            this.next = null;
            this.lastEntryBlock = -1;
        }

        private boolean isMatch(IdxEntry entry) {
            return entry.isAvailable() && entry.getAlloc() >= length;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            IdxEntry entry = new IdxEntry(buffer, ++lastEntryBlock);
            try {
                while (entry.offset < getFileSize()) {
                    if (isMatch(entry)) {
                        next = entry;
                        lastEntryBlock = entry.getBlock();
                        return true;
                    }
                    entry.offset += BLOCK_SIZE;
                }
            } catch (IOException e) {
                logger.error("Scanning error", e);
                return false;
            }
            return false;
        }

        public int getOffset() {
            if (hasNext()) {
                return next.offset;
            }
            throw new NoSuchElementException();
        }

        @Override
        public IdxEntry next() {
            if (hasNext()) {
                try {
                    return next;
                } finally {
                    next = null;
                }

            }
            throw new NoSuchElementException();
        }

        @Override
        public Boolean call() {
            if (hasNext() && isMatch(next)) {
                try {
                    next.setDeleted(false);
                    next.setInitialized(false);
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return false;
        }

    }

    /**
     * Searches for an empty block of specified length.
     * If an empty block is found is is marked as not deleted and not initialized.
     * @param length the minimum length of the block.
     * @return the IdxEntry that will accept the length or null if none found.
     * @throws IOException
     */
    public IdxEntry search(int length) throws IOException {
        ByteBuffer buffer = getBuffer();
        Scanner scanner = new Scanner(buffer, length);

        while (scanner.hasNext()) {
            if (sync(scanner, scanner.getOffset(), BLOCK_SIZE, 4)) {
                return scanner.next();
            }
            // skip the last result
            scanner.next();
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
            IdxEntry idxEntry = new IdxEntry(getBuffer(), lock.getStart() / BLOCK_SIZE);
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
        IdxEntry idxEntry = new IdxEntry(getBuffer(), block);
        idxEntry.setDeleted(true);
    }
}
