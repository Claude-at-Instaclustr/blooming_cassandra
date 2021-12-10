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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.mem.FlatBloomingIndex;
import com.instaclustr.cassandra.bloom.idx.mem.FlatBloomingIndexSerde;
import com.instaclustr.cassandra.bloom.idx.mem.tables.AbstractTable.RangeLock;

public abstract class AbstractTable implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTable.class);

    private final File file;
    private final RandomAccessFile raFile;
    private final int blockSize;
    private final Map<Integer,ReentrantLock> blockLocks = new ConcurrentHashMap<Integer,ReentrantLock>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final Runnable blockLogsCleanup = new Runnable() {
        @Override
        public void run() {
            blockLocks.forEach( (k,v) -> {if (!(v.isLocked()||v.hasQueuedThreads())) { blockLocks.remove(k);}});
        }
    };
    /**
     * Constructor
     * @param file The file to use.
     * @param blockSize the blocksize to extend the file by.
     * @throws IOException on IO error.
     */
    public AbstractTable(File file, int blockSize) throws IOException {
        this.file = file;
        this.file.createNewFile();
        this.raFile = new RandomAccessFile(file, "rw");
        this.blockSize = blockSize;
        this.executor.scheduleWithFixedDelay(blockLogsCleanup, 1000*5*50, 5, TimeUnit.MINUTES);
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
        if (size < blockSize) {
            size = blockSize;
        }
        return fileChannel.map(mode, 0, size);

    }

//    protected <T> T sync( Supplier<T> action, int block ) throws SyncFailedException, TimeoutException {
//        return sync( action, block, 1 );
//    }
//
//    protected <T> T sync( Supplier<T> action, int block, int retryCount ) throws SyncFailedException, TimeoutException {
//        return sync( action, block, block, retryCount );
//    }

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
    protected <T> T sync( Supplier<T> action, int startByte, int length, int retryCount ) throws SyncFailedException, TimeoutException {
        int count=retryCount;
        while (count>0) {
            try (RangeLock lock = lock( startByte, startByte+length-1 )){
                if (lock.hasLock()) {
                    return action.get() ;
                }
                count--;
            } catch (Exception e) {
                throw new SyncFailedException( e.getMessage() );
            }
        }
        throw new TimeoutException( "Unable to lock "+this );
    }

    private final synchronized RangeLock lock( int start, int stop ) {
        int blockStart = start/blockSize;
        int blockStop = stop/blockSize;
        RangeLock rangeLock = new RangeLock();
        for (int i=blockStart;i<=blockStop;i++) {
            ReentrantLock lock = blockLocks.get( i );
            if (lock == null) {
                lock = new ReentrantLock();
                if (rangeLock.add(lock)) {
                    blockLocks.put( i, lock);
                } else {
                    return rangeLock;
                }
            } else {
                if (!rangeLock.add( lock )) {
                    return rangeLock;
                }
            }
        }
        return rangeLock;
    }

    /**
     * Creates a new writable buffer that in guaranteed to have at least the number of blocks specified.
     * @param blocks the minimum number of blocks.
     * @return the new ByteBuffer.
     * @throws IOException on IO error.
     */
    protected final synchronized ByteBuffer ensureBlock(long blocks) throws IOException {
        long fileBlocks = raFile.length() / blockSize;
        if (fileBlocks<=blocks) {
            return extendBuffer( blocks+1-fileBlocks );
        }
        return getWritableBuffer();
    }
    /**
     * Crates a new writable ByteBuffer that is 1 blocksize longer than the current file
     * size.
     * @return new ByteBuffer
     * @throws IOException on IOError
     */
    protected final ByteBuffer extendBuffer() throws IOException{
        return extendBuffer( 1 );
    }

    /**
     * Crates a new writable ByteBuffer that is 1 blocksize longer than the current file
     * size.
     * @param blocks the number of blocks to extend the buffer by.
     * @return new ByteBuffer
     * @throws IOException on IOError
     */
    protected synchronized final ByteBuffer extendBuffer(long blocks) throws IOException{
        FileChannel fileChannel = raFile.getChannel();
        // Get direct long buffer access using channel.map() operation

        long size = fileChannel.size() + (blocks*blockSize);
        return fileChannel.map(MapMode.READ_WRITE, 0, size);
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
        try {
            c.close();
        } catch (Exception e) {
            logger.error(String.format("Exception thrown while closing %s", c), e);
        }
    }

    class RangeLock implements AutoCloseable  {
        List<ReentrantLock> locks = new ArrayList<ReentrantLock>();

        boolean add( ReentrantLock lock ) {
            try {
                if (lock.tryLock(500, TimeUnit.MILLISECONDS)) {
                    locks.add( lock );
                    return true;
                }
            } catch (InterruptedException e) {
                logger.warn( "Unable to get lock");
            }
            locks.forEach( ReentrantLock::unlock );
            locks.clear();
            return false;
        }

        public boolean hasLock() {
            return ! locks.isEmpty();
        }

        public void release() {
            for (ReentrantLock lock : locks)
            {
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
}
