package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import com.instaclustr.cassandra.bloom.idx.mem.FlatBloomingIndexSerde;

public abstract class AbstractTable implements AutoCloseable {

    protected final File file;
    protected final RandomAccessFile raFile;
    private ByteBuffer readBuffer;

    public AbstractTable(File file) throws IOException {
        this.file = file;
        this.raFile = new RandomAccessFile(file, "rw");
        this.readBuffer = getBuffer(FileChannel.MapMode.READ_ONLY).asReadOnlyBuffer();
    }

    @Override
    public String toString() {
        return file.getAbsolutePath();
    }

    protected long getFileSize() throws IOException {
        return raFile.length();
    }

    @Override
    public void close() throws IOException {
        this.raFile.close();
        this.readBuffer = null;
    }

    private final ByteBuffer getBuffer(FileChannel.MapMode mode) throws IOException {
        FileChannel fileChannel = raFile.getChannel();
        // Get direct long buffer access using channel.map() operation
        return fileChannel.map(mode, 0, fileChannel.size());

    }

    protected final ByteBuffer getBuffer() throws IOException {
        return readBuffer.duplicate();
    }

    protected final ByteBuffer getWritableBuffer() throws IOException {
        return getBuffer(FileChannel.MapMode.READ_WRITE);
    }

    protected final IntBuffer getIntBuffer() throws IOException {
        return readBuffer.asIntBuffer().duplicate();
    }

    protected final IntBuffer getWritableIntBuffer() throws IOException {
        return getBuffer(FileChannel.MapMode.READ_WRITE).asIntBuffer();
    }

    protected final LongBuffer getLongBuffer() throws IOException {
        return readBuffer.asLongBuffer().duplicate();
    }

    protected final LongBuffer getWritableLongBuffer() throws IOException {
        return getBuffer(FileChannel.MapMode.READ_WRITE).asLongBuffer();
    }

    public static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception e) {
            FlatBloomingIndexSerde.logger.error(String.format("Exception thrown while closing %s", c), e);
        }
    }

}
