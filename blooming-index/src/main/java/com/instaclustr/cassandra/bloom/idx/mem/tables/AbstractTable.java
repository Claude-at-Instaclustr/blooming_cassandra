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
import java.util.Map.Entry;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTable implements AutoCloseable {

    @FunctionalInterface
    interface Func {
        void exec() throws Exception;
    }

    private final File file;
    private final RandomAccessFile raFile;
    private final int blockSize;
    private final Map<Integer, ReentrantLock> blockLocks = new ConcurrentHashMap<Integer, ReentrantLock>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final Runnable blockLogsCleanup = new Runnable() {
        @Override
        public void run() {
            blockLocks.forEach((k, v) -> {
                if (!(v.isLocked() || v.hasQueuedThreads())) {
                    blockLocks.remove(k);
                }
            });
        }
    };

    /**
     * Constructor
     * @param file The file to use.
     * @param blockSize the blocksize in bytes to extend the file by.
     * @throws IOException on IO error.
     */
    public AbstractTable(File file, int blockSize) throws IOException {
        this.file = file;
        this.file.createNewFile();
        this.raFile = new RandomAccessFile(file, "rw");
        this.blockSize = blockSize;
        this.executor.scheduleWithFixedDelay(blockLogsCleanup, 1000 * 5 * 50, 5, TimeUnit.MINUTES);
    }

    protected int getBlockSize() {
        return blockSize;
    }

    protected int getLockCount() {
        return blockLocks.size();
    }

    protected void getLockedBlocks( IntConsumer consumer ) {
        blockLocks.forEach( (k,v) -> {if (v.isLocked()) { consumer.accept(k);}} );
    }

    @Override
    public String toString() {
        return file.getAbsolutePath();
    }

    /**
     * Gets the length of the file.
     * @return the length of the file.
     * @throws IOException on IO error.
     */
    protected long getFileSize() throws IOException {
        return raFile.length();
    }

    @Override
    public void close() throws IOException {
        this.executor.shutdownNow();
        this.raFile.close();

    }

    private final ByteBuffer getBuffer(MapMode mode) throws IOException {
        FileChannel fileChannel = raFile.getChannel();
        // Get direct long buffer access using channel.map() operation

        long size = fileChannel.size();
//        if (size < blockSize) {
//            size = blockSize;
//        }
        return fileChannel.map(mode, 0, size);

    }

    /**
     * Execute the action after locking the byte range.  it is expected that the action will
     * update the buffer.
     *
     * @param <T> The value type to be returned
     * @param action the Supplier that will perform the action.
     * @param startByte the starting byte that the action may modify
     * @param length the number of consecutive bytes the action may modify.
     * @param retryCount the number of times to retry the lock before failing.
     * @return the result of the action.
     * @throws SyncFailedException if the execution throws an excepton.
     * @throws TimeoutException if the lock could not be established.
     */
    protected <T> T sync(Supplier<T> action, int startByte, int length, int retryCount) throws IOException {
        try (RangeLock lock = getLock(startByte, length, retryCount)) {
            return action.get();
        }
    }

    /**
     * Get a range lock over the specified range.
     *
     * <em>RangeLock must be closed or released and should only be held for a short time</em>
     *
     * @param startByte the starting byte that the action may modify
     * @param length the number of consecutive bytes the action may modify.
     * @param retryCount the number of times to retry the lock before failing.
     * @return the RangeLock
     * @throws SyncFailedException if the
     * @throws OutputTimeoutException if the lock could not be established.
     */
    protected final RangeLock getLock(int startByte, int length, int retryCount) throws OutputTimeoutException {
        int count = retryCount;
        while (count > 0) {
            RangeLock lock = lock(startByte, startByte + length - 1);
            if (lock.hasLock()) {
                return lock;
            }
            lock.close();
            count--;
        }
        throw new OutputTimeoutException("Unable to lock " + this);
    }

    private final synchronized RangeLock lock(int start, int stop) {
        int blockStart = start / blockSize;
        int blockStop = stop / blockSize;
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

    protected boolean hasBlock( int block ) throws IOException {
        return (raFile.length() / blockSize) > block;
    }
    /**
     * Creates a new writable buffer that in guaranteed to have at least the number of blocks specified.
     * @param blocks the minimum number of blocks.
     * @return true if a new block was created
     * @throws IOException on IO error.
     */
    protected final synchronized boolean ensureBlock(long blocks) throws IOException {
        long fileBlocks = raFile.length() / blockSize;
        if (fileBlocks <= blocks) {
            extendBuffer(blocks + 1 - fileBlocks);
            return true;
        }
        return false;
    }

    /**
     * Crates a new writable ByteBuffer that is 1 blocksize longer than the current file
     * size.
     * @return first byte of the new block
     * @throws IOException on IOError
     */
    protected final int extendBuffer() throws IOException {
        return extendBuffer(1);
    }

    /**
     * Crates a new writable ByteBuffer that is 1 blocksize longer than the current file
     * size.
     * @param blocks the number of blocks to extend the buffer by.
     * @return first byte of the new block.
     * @throws IOException on IOError
     */
    protected synchronized final int extendBuffer(long blocks) throws IOException {
        FileChannel fileChannel = raFile.getChannel();
        // Get direct long buffer access using channel.map() operation

        int result = (int) fileChannel.size();
        long size = fileChannel.size() + (blocks * blockSize);
        fileChannel.map(MapMode.READ_WRITE, 0, size);
        return result;
    }

    /**
     * Crates a new writable ByteBuffer that is {@code bytes} longer than the current file
     * size.
     * @param blocks the number of blocks to extend the buffer by.
     * @return first byte of the new block.
     * @throws IOException on IOError
     */
    protected synchronized final int extendBytes(int bytes) throws IOException {
        FileChannel fileChannel = raFile.getChannel();
        // Get direct long buffer access using channel.map() operation

        int result = (int) fileChannel.size();
        long size = fileChannel.size() + bytes;
        fileChannel.map(MapMode.READ_WRITE, 0, size);
        return result;
    }

    /**
     * Crates a new read only ByteBuffer.
     * @return new ByteBuffer
     * @throws IOException on IOError
     */
    protected final ByteBuffer getBuffer() throws IOException {
        return getBuffer(MapMode.READ_ONLY).asReadOnlyBuffer();
    }

    /**
     * Crates a new writable ByteBuffer.
     * @return new ByteBuffer
     * @throws IOException on IOError
     */
    protected final ByteBuffer getWritableBuffer() throws IOException {
        return getBuffer(MapMode.READ_WRITE);
    }

    /**
     * Crates a new read only IntBuffer.
     * @return new IntBuffer
     * @throws IOException on IOError
     */
    protected final IntBuffer getIntBuffer() throws IOException {
        return getBuffer().asIntBuffer().duplicate();
    }

    /**
     * Crates a new writable IntBuffer.
     * @return new IntBuffer
     * @throws IOException on IOError
     */
    protected final IntBuffer getWritableIntBuffer() throws IOException {
        return getBuffer(MapMode.READ_WRITE).asIntBuffer();
    }

    /**
     * Crates a new read only LongBuffer.
     * @return new LongBuffer
     * @throws IOException on IOError
     */
    protected final LongBuffer getLongBuffer() throws IOException {
        return getBuffer().asLongBuffer().duplicate();
    }

    /**
     * Crates a new writable LongBuffer.
     * @return new LongBuffer
     * @throws IOException on IOError
     */
    protected final LongBuffer getWritableLongBuffer() throws IOException {
        return getBuffer(MapMode.READ_WRITE).asLongBuffer();
    }

    /**
     * Closes an AutoCloseable instance and ignores any exceptions.
     * Any exceptions are logged as errors.
     * @param c the instance to close.
     */
    public static void closeQuietly(AutoCloseable c) {
        execQuietly(() -> c.close(), AbstractTable.class);
    }

    private static void execQuietly(Func fn, Class<?> caller ) {
        try {
            fn.exec();
        } catch (Exception e) {
            LoggerFactory.getLogger(caller)
                    .error(String.format("Exception thrown while executing quietly %s", e), e);
        }
    }

    private Logger getLogger() {
        return LoggerFactory.getLogger(this.getClass());
    }

    protected void execQuietly(Stack<Func> undo) {
        while (!undo.empty()) {
            execQuietly(undo.pop());
        }
    }

    protected void execQuietly(Func fn) {
        execQuietly( fn, this.getClass() );
    }

    class RangeLock implements AutoCloseable {
        /**
         * The starting block
         */
        private int start;
        private List<ReentrantLock> locks = new ArrayList<ReentrantLock>();

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

        public boolean hasLock() {
            return !locks.isEmpty();
        }

        public void release() {
            for (ReentrantLock lock : locks) {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }

        @Override
        public void close() {
            release();
        }

    }

    public static class OutputTimeoutException extends IOException {

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
}
