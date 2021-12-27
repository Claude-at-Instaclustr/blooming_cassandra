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
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base table for random access file access.
 *
 * <p>The base table aligns data in Blocks as defined by the blockSize parameter.
 * The table uses blocks to perform locking operations for updates.</p>
 *
 * <p>The base table also allows byte oriented operations.  Extending the table by
 * bytes can result in tables that do not align on the block boundary.  This does not
 * cause a problem fo the table operation.  However users should be aware that block
 * extension and detection operates on complete blocks so mixing byte operations and
 * block operations is discouraged.
 * </p>
 *
 *
 */
public class BaseTable implements AutoCloseable {
    /**
     * Convenience static to specify Read only access.
     */
    public final static boolean READ_ONLY = true;
    /**
     * Convenience static to specify Read/Write only access.
     */
    public final static boolean READ_WRITE = false;
    /**
     * The file this Base table is operating on.
     */
    private final File file;
    /**
     * The random access file reading the File.
     */
    private final RandomAccessFile raFile;
    /**
     * The size of the blocks in the file.  Blocks are used to for locking operations.
     */
    private final int blockSize;
    /**
     * The soze of blocks during an extension.  If not explicitly specified this is the same as the blockSize.
     */
    private int extensionBlockSize;
    /**
     * The recently used locks.
     */
    private final Map<Integer, ReentrantLock> blockLocks = new ConcurrentHashMap<Integer, ReentrantLock>();
    /**
     * The table executor to perform clean up tasks and other table based operations.
     */
    protected final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    /**
     * A list of Func to call when an extension to the file is performed.
     */
    private final List<Func> extendNotify = new CopyOnWriteArrayList<Func>();
    /**
     * True if this table is read only.
     */
    private final boolean readOnly;
    /**
     * A runnable to clean up the old block locks.
     */
    protected final Runnable blockLogsCleanup = new Runnable() {
        @Override
        public void run() {
            blockLocks.forEach((k, v) -> {
                if (!(v.isLocked() || v.hasQueuedThreads())) {
                    try (RangeLock lock = lock(k * blockSize, blockSize)) {
                        if (lock.hasLock()) {
                            blockLocks.remove(k);
                        }
                    }
                }
            });
        }
    };
    /**
     * The byte buffer associated with this table.
     */
    private ByteBuffer buffer;

    /**
     * Constructor
     * @param file The file to use.
     * @param blockSize the size of the block in the table.
     * @throws IOException on IO error.
     */
    protected BaseTable(File file, int blockSize, boolean readOnly) throws IOException {
        this.readOnly = readOnly;
        this.file = file;
        this.file.createNewFile();
        this.raFile = new RandomAccessFile(file, readOnly ? "r" : "rw");
        this.blockSize = blockSize;
        this.extensionBlockSize = blockSize;
        this.executor.scheduleWithFixedDelay(blockLogsCleanup, 1000 * 5 * 50, 5, TimeUnit.MINUTES);
        FileChannel fileChannel = raFile.getChannel();
        long size = fileChannel.size();
        this.buffer = fileChannel.map(readOnly ? MapMode.READ_ONLY : MapMode.READ_WRITE, 0, size);
    }

    /**
     * Execute a func using the executor associated with this table.
     * @param fn the Func to execute.
     * @return the Future for the execution.
     */
    public Future<?> exec(Func fn) {
        return executor.submit(fn.asCallable());
    }

    /**
     * Execute a func and requeue it if it failed deu to an OutputTimeoutException.
     * @param fn the Func to execute.
     */
    public void requeue(Func fn) {
        executor.submit(() -> {
            try {
                fn.call();
            } catch (OutputTimeoutException e) {
                requeue(fn);
            } catch (Exception e) {
                getLogger().error("Error during requeue", e);
            }
        });
    }

    /**
     * set the extension block size.  This is the number of bytes for each block during an extension.  If not set it
     * defaults to the blockSize.
     * @param extensionBlockSize The extension block size.
     */
    protected void setExtensionBlockSize(int extensionBlockSize) {
        this.extensionBlockSize = extensionBlockSize;
    }

    /**
     * Register a Func for notification when an extension occurs.
     * @param fn the Function to call when an extension occurs.
     */
    protected void registerExtendNotification(Func fn) {
        extendNotify.add(fn);
    }

    /**
     * Get the current block size.
     * @return
     */
    protected int getBlockSize() {
        return blockSize;
    }

    /**
     * Like a {@code Callable<Void>} but does not require a return value.
     * @See Callable
     */
    @FunctionalInterface
    public interface Func {

        /**
         * Executes some code that can thrown an exception but does not return
         * a value.
         * @throws Exception on error.
         */
        void call() throws Exception;

        /**
         * Converts Func to a {@code Callable<Void>} for callable application.
         * @return A {@code Callable<Void>} that calls this.
         */
        default Callable<Void> asCallable() {
            return new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    Func.this.call();
                    return null;
                }
            };
        }
    }

    /**
     * Executes a Func and retries it if it fails due to an OutputTimeoutException.
     * @param fn the function to execute.
     * @throws Exception for non OutputTimeoutException errors.
     */
    public void retryOnTimeout(Func fn) throws Exception {
        retryOnTimeout(fn.asCallable());
    }

    /**
     * Executes a Callable and retries it if it fails due to an OutputTimeoutException.
     * @param <T> the return type.
     * @param fn the Callable to execute.
     * @throws Exception for non OutputTimeoutException errors.
     */
    public <T> T retryOnTimeout(Callable<T> fn) throws Exception {
        while (true) {
            try {
                return fn.call();
            } catch (OutputTimeoutException e) {
                getLogger().debug("Timeout  executing {}, trying again", fn);
            } catch (Exception e) {
                getLogger().warn("Error {} attempting {}", e, fn);
                throw e;
            }
        }
    }

    /**
     * Returns the number of active block locks on the table.
     * <p>
     * Active block locks are ones that were in use recently.  They system will
     * remove them after a period of inactivity, so active locks may contain locks
     * that are not being held at the moment.
     *  </p>
     * @return the number of active block locks.
     */
    protected int getLockCount() {
        return blockLocks.size();
    }

    /**
     * Enumerates blocks with currently held locks.
     * <p>
     * The enumeration includes locks that were active when this thread looked.  Another
     * thread may have released the lock since it was enumerated.
     * </p>
     *
     * @param consumer The consumer to accept the block numbers.
     */
    protected void getLockedBlocks(IntConsumer consumer) {
        blockLocks.forEach((k, v) -> {
            if (v.isLocked()) {
                consumer.accept(k);
            }
        });
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.getClass().getSimpleName(), file.getAbsolutePath());
    }

    /**
     * Gets the length of the file.
     * @return the length of the file.
     * @throws IOException on IO error.
     */
    protected long getFileSize() throws IOException {
        return raFile.length();
    }

    /**
     * Drops the index.  All data are deleted.
     */
    public void drop() {
        closeQuietly();
        this.file.delete();
    }

    @Override
    public void close() throws IOException {
        this.executor.shutdownNow();
        try {
            this.executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            getLogger().error("Timeout waiting for executor to stop", e);
        }
        this.raFile.close();
    }

    /**
     * Execute the action after locking the byte range.  it is expected that the action will
     * update the buffer.
     *
     * @param <T> The value type to be returned
     * @param action the Callable that will perform the action.
     * @param startByte the starting byte that the action may modify
     * @param length the number of consecutive bytes the action may modify.
     * @param retryCount the number of times to retry the lock before failing.
     * @return the result of the action.
     * @throws IOException on IO Error
     * @throws SyncFailedException if the execution throws a non IOExcepton.
     * @throws OutputTimeoutException if the lock could not be established.
     */
    protected <T> T sync(Callable<T> action, int startByte, int length, int retryCount) throws IOException {
        try (RangeLock lock = getLock(startByte, length, retryCount)) {
            try {
                return action.call();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new SyncException(e);
            }
        }
    }

    /**
     * Execute the action after locking the byte range.  it is expected that the action will
     * update the buffer.
     *
     * @param action the Func that will perform the action.
     * @param startByte the starting byte that the action may modify
     * @param length the number of consecutive bytes the action may modify.
     * @param retryCount the number of times to retry the lock before failing.
     * @throws IOException on IO Error
     * @throws SyncFailedException if the execution throws a non IOExcepton.
     * @throws OutputTimeoutException if the lock could not be established.
     * @see #sync(Callable, int, int, int)
     */
    protected void sync(Func action, int startByte, int length, int retryCount) throws IOException {
        sync(action.asCallable(), startByte, length, retryCount);
    }

    /**
     * checks that a position is on a block boundary.
     *
     * If the position is not on a buffer boundary a warning is printed in the log.
     * @param position the position to check.
     * @param blockSize the expected size of blocks in the system.
     * @return {@code true} if the position aligned with the block size, {@code false} otherwise.
     */
    protected boolean checkBlockAlignment(long position, int blockSize) {
        if ((position % blockSize) != 0) {
            long lower = position - (position % blockSize);
            long upper = lower + blockSize;
            getLogger().warn("position does not allign with block size {} should be {} or {}.", position, lower, upper);
            return false;
        }
        return true;
    }

    /**
     * Check that the parameter is greater than or equal to zero.
     * @param value the value to check
     * @param name the name of the parameter
     */
    protected void checkGEZero(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(String.format("%s (%s) may not be less than zero (0)", name, value));
        }
    }

    /**
     * Check that the parameter is greater than zero.
     * @param value the value to check
     * @param name the name of the parameter
     */
    protected void checkGTZero(long count, String name) {
        if (count <= 0) {
            throw new IllegalArgumentException(
                    String.format("%s (%s) may not be less than or equal to zero (0)", name, count));
        }
    }

    /**
     * Get a range lock over the specified range.
     *
     * <em>RangeLock must be closed or released and should only be held for a short time</em>
     *
     * @param startByte the starting byte that the action may modify
     * @param length the number of consecutive bytes the action may modify.
     * @param retryCount the number of times to retry the lock before failing.  Lock will be attempted
     * at least once.
     * @return the RangeLock
     * @throws OutputTimeoutException if the lock could not be established.
     * @throws IllegalArgumentException If {@code startByte < 0}.
     * @throws IllegalArgumentException If {@code length <= 0}.
     * @throws IllegalArgumentException If {@code retryCount < 0}.
     */
    protected final RangeLock getLock(int startByte, int length, int retryCount) throws OutputTimeoutException {
        checkGEZero(startByte, "startByte");
        checkGTZero(length, "length");
        checkGEZero(retryCount, "retryCount");
        int count = retryCount;
        do {
            RangeLock lock = lock(startByte, startByte + length - 1);
            if (lock.hasLock()) {
                return lock;
            }
            lock.close();
            count--;
        } while (count > 0);
        throw new OutputTimeoutException("Unable to lock " + this);
    }

    /**
     * Get the block number that contains the byte count.
     * @param byteCount the byte count to locate.
     * @return the Block number containing the byte count.
     */
    public long blockNumber(long byteCount) {
        return byteCount / blockSize;
    }

    /**
     * Get the block number that contains the byte count.
     * @param byteCount the byte count to locate.
     * @return the Block number containing the byte count.
     */
    public int blockNumber(int byteCount) {
        return byteCount / blockSize;
    }

    /**
     * Calculates and locks blocks.
     * @param start the starting byte
     * @param stop the ending byte
     * @return a RangeLock containing the locks for all the blocks or
     *  none of the locks.
     */
    private final synchronized RangeLock lock(int start, int stop) {
        int blockStart = blockNumber(start);
        int blockStop = blockNumber(stop);
        RangeLock rangeLock = new RangeLock(start);
        for (int i = blockStart; i <= blockStop; i++) {
            ReentrantLock lock = blockLocks.get(i);
            if (lock == null) {
                lock = new ReentrantLock();
                if (rangeLock.add(lock)) {
                    blockLocks.put(i, lock);
                } else {
                    return rangeLock;
                }
            } else {
                if (!rangeLock.add(lock)) {
                    return rangeLock;
                }
            }
        }
        return rangeLock;
    }

    /**
     * Determines if the block has been created in the table.  The block is considered created
     * only if the entire block has been created.  Partial blocks created with extendBytes
     * are not counted.
     * @param block the block the check. (0 based)
     * @return {@code true} if the block is in the table, false otherwise.
     * @throws IOException on IO error
     */
    protected boolean hasBlock(int block) throws IOException {
        return blockNumber(raFile.length()) > block;
    }

    /**
     * Creates a new writable buffer that in guaranteed to have at least the number of blocks specified.
     * @param blocks the minimum number of blocks.
     * @return true if a new block was created
     * @throws IOException on IO error.
     */
    protected final synchronized boolean ensureBlock(long blocks) throws IOException {
        if (blocks == 0) {
            return true;
        }
        long fileBytes = raFile.length();
        if (fileBytes == 0) {
            extendBuffer(blocks);
            return true;
        } else {
            long requiredBytes = blocks * extensionBlockSize;
            if (fileBytes < requiredBytes) {
                extendBuffer(blocks - (fileBytes / extensionBlockSize));
                return true;
            }
        }
        return false;
    }

    /**
     * Crates a new writable ByteBuffer that is 1 block longer than the current file
     * size.
     * @return offset of the first byte of the new block
     * @throws IOException on IOError
     */
    protected final int extendBuffer() throws IOException {
        return extendBuffer(1);
    }

    /**
     * Creates a new writable ByteBuffer that is blocks longer than the current file
     * size.  if the buffer is not aligned on a block boundary it will continue to be
     * unaligned by the same number of bytes after this call.
     *
     * @param blocks the number of blocks to extend the buffer by.
     * @return offset of the first byte of the new block.
     * @throws IOException on IOError
     * @throws IllegalArgumentException If {@code blocks < 0}.
     */
    protected synchronized final int extendBuffer(long blocks) throws IOException {
        checkGEZero(blocks, "blocks");

        FileChannel fileChannel = raFile.getChannel();
        // Get direct long buffer access using channel.map() operation
        int result = (int) fileChannel.size();
        long size = fileChannel.size() + (blocks * extensionBlockSize);
        buffer = fileChannel.map(readOnly ? MapMode.READ_ONLY : MapMode.READ_WRITE, 0, size);
        extendNotify.forEach(f -> execQuietly(f));
        return result;
    }

    /**
     * Crates a new writable ByteBuffer that is {@code bytes} longer than the current file
     * size.
     * @param bytes the number of bytes to extend the buffer by.
     * @return the offset of the first byte new byte in the file.
     * @throws IOException on IOError
     * @throws IllegalArgumentException If {@code bytes < 0}.
     */
    protected synchronized final int extendBytes(int bytes) throws IOException {
        checkGEZero(bytes, "bytes");

        FileChannel fileChannel = raFile.getChannel();
        // Get direct long buffer access using channel.map() operation

        int result = (int) fileChannel.size();
        long size = fileChannel.size() + bytes;
        buffer = fileChannel.map(readOnly ? MapMode.READ_ONLY : MapMode.READ_WRITE, 0, size);
        extendNotify.forEach(f -> execQuietly(f));
        return result;
    }

    /**
     * Creates a new read only ByteBuffer.
     * @return new ByteBuffer
     * @throws IOException on IOError
     */
    protected final ByteBuffer getBuffer() throws IOException {
        return buffer.asReadOnlyBuffer();
    }

    /**
     * Crates a new writable ByteBuffer.
     * @return new ByteBuffer
     * @throws IOException on IOError
     */
    protected final ByteBuffer getWritableBuffer() throws IOException {
        return buffer.duplicate();
    }

    /**
     * Crates a new read only IntBuffer.
     * @return new IntBuffer
     * @throws IOException on IOError
     */
    protected final IntBuffer getIntBuffer() throws IOException {
        return getBuffer().asIntBuffer();
    }

    /**
     * Crates a new writable IntBuffer.
     * @return new IntBuffer
     * @throws IOException on IOError
     */
    protected final IntBuffer getWritableIntBuffer() throws IOException {
        return getWritableBuffer().asIntBuffer();
    }

    /**
     * Crates a new read only LongBuffer.
     * @return new LongBuffer
     * @throws IOException on IOError
     */
    protected final LongBuffer getLongBuffer() throws IOException {
        return getBuffer().asLongBuffer();
    }

    /**
     * Crates a new writable LongBuffer.
     * @return new LongBuffer
     * @throws IOException on IOError
     */
    protected final LongBuffer getWritableLongBuffer() throws IOException {
        return getWritableBuffer().asLongBuffer();
    }

    /**
     * Closes an AutoCloseable instance and ignores any exceptions.
     * Any exceptions are logged as errors.
     * @param closable the instance to close.
     */
    public static void closeQuietly(AutoCloseable closable) {
        execQuietly(() -> closable.close(), BaseTable.class);
    }

    /**
     * Closes an this instance and ignores any exceptions.
     * Any exceptions are logged as errors.
     */
    public void closeQuietly() {
        execQuietly(this::close);
    }

    /**
     * Executes a func ignoring any exception thrown.
     * All exceptions are logged as errors in using the logger for the
     * class calling this method.
     * @param func the Func to execute.
     * @param caller the class calling this exec method.
     */
    private static void execQuietly(Func func, Class<?> caller) {
        execQuietly(func.asCallable(), caller);
    }

    /**
     * Executes a {@code Callable} ignoring any exception thrown.
     * All exceptions are logged as errors in using the logger for the
     * class calling this method.
     * @param func the Func to execute.
     * @param caller the class calling this exec method.
     * @return the result of the Callable or @{code null} if an exception was thrown.
     */
    private static <T> T execQuietly(Callable<T> fn, Class<?> caller) {
        try {
            return fn.call();
        } catch (Exception e) {
            LoggerFactory.getLogger(caller).error(String.format("Exception thrown while executing quietly %s", e), e);
            return null;
        }
    }

    /**
     * get the logger for the class that is actually executing.
     * @return the current logger.
     */
    private Logger getLogger() {
        return LoggerFactory.getLogger(this.getClass());
    }

    /**
     * Execute a function quietly.  All exceptions are logged as errors but are ignored.
     * @param fn the function to execute;
     * @see Func
     */
    protected void execQuietly(Func fn) {
        execQuietly(fn, this.getClass());
    }

    /**
     * A set of locks that covers a range of consecutive blocks.
     *
     * Will contain all of the necessary locks or none of them.
     *
     */
    class RangeLock implements AutoCloseable {
        /**
         * The starting block
         */
        private int start;
        /**
         * list of block locks this range lock holds.
         */
        private List<ReentrantLock> locks = new ArrayList<ReentrantLock>();

        /**
         * Constructor.
         * @param start The starting byte.
         */
        RangeLock(int start) {
            this.start = start;
        }

        /**
         * The byte at which this lock starts.
         * @return The byte at which this lock starts.
         */
        public int getStart() {
            return start;
        }

        /**
         * Add another lock to this range.
         * If the lock can not be established all of the locks in the collection are
         * released.
         * @param lock the lock to add
         * @return {@code true} if the lock can be established within 500 milliseconds, {@code false} it it can not.
         */
        boolean add(ReentrantLock lock) {
            try {
                if (lock.tryLock(500, TimeUnit.MILLISECONDS)) {
                    locks.add(lock);
                    return true;
                }
            } catch (InterruptedException e) {
                getLogger().warn("Unable to get lock");
            }
            locks.forEach(ReentrantLock::unlock);
            locks.clear();
            return false;
        }

        /**
         * Returns {@code true} if the RangeLock is holding any locks.
         * @return {@code true} if this holds any locks, {@code false} otherwise.
         */
        public boolean hasLock() {
            return !locks.isEmpty();
        }

        /**
         * Returns the number of locks held byt this RangeLock..
         * @return the number of locks held.
         */
        public int lockCount() {
            return locks.size();
        }

        /**
         * Releasees all the locks held by this thread.
         */
        public void release() {
            for (ReentrantLock lock : locks) {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }

        /**
         * Closes this RangeLock by releasing all the locks.
         */
        @Override
        public void close() {
            release();
        }

    }

    /**
     * An exception that is thrown when a timeout occurs while trying to
     * establish a lock.
     *
     */
    public static class OutputTimeoutException extends IOException {

        /**
         *
         */
        private static final long serialVersionUID = 6817824021982164976L;

        public OutputTimeoutException() {
            super();
        }

        public OutputTimeoutException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }

        public OutputTimeoutException(String arg0) {
            super(arg0);
        }

        public OutputTimeoutException(Throwable arg0) {
            super(arg0);
        }

    }

    /**
     * An exception that is thrown when an Callable throws an Exception while
     * executing within a sync call.
     *
     * @See {@link BaseTable#sync(Callable, int, int, int)}
     * @See {@link BaseTable#sync(Func, int, int, int)}
     */
    public static class SyncException extends IOException {

        /**
         *
         */
        private static final long serialVersionUID = 5619395355954428585L;

        /**
         * Constructor
         * @param cause the exception that occured in the sync.
         */
        public SyncException(Exception cause) {
            super(cause);
        }

    }
}
