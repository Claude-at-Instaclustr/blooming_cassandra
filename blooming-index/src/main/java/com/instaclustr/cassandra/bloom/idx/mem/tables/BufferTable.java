package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

public class BufferTable extends AbstractTable {

    public static final int UNSET = -1;
    final IdxMap idxTable;
    final BufferTableIdx keyTableIdx;

    @Override
    public void close() throws IOException {
        closeQuietly(idxTable);
        closeQuietly(keyTableIdx);
        super.close();
    }

    @Override
    public String toString() {
        return "KeyTable: " + super.toString();
    }

    public BufferTable(File file, int blockSize) throws IOException {
        super(file, blockSize);
        File idxFile = new File(file.getParentFile(), file.getName() + "_idx");
        idxTable = new IdxMap(idxFile);
        File keyIdxFile = new File(file.getParentFile(), file.getName() + "_keyidx");
        keyTableIdx = new BufferTableIdx(keyIdxFile);
    }

    public ByteBuffer get(int idx) throws IOException {
        if (! idxTable.hasBlock(idx)) {
            return null;
        }
        IdxMap.MapEntry mapEntry = idxTable.get(idx);
        if (!mapEntry.isInitialized()) {
            return null;
        }
        BufferTableIdx.IdxEntry idxEntry = keyTableIdx.get(mapEntry.getKeyIdx());
        if (idxEntry.isDeleted()) {
            return null;
        }
        ByteBuffer buff = getBuffer();
        int pos = idxEntry.getOffset();
        buff.position(pos);
        buff.limit(pos + idxEntry.getLen());
        return buff;
    }

    /**
     * Creates Adds a block to the buffer table.
     * The returned IdxEntry is not initializes, so it is not visible in searches
     * calling method should set the initialized state when ready.
     * @param buff the buffer to add to the table.
     * @return the IdxEntry for the buffer.
     * @throws IOException
     */
    private BufferTableIdx.IdxEntry add(ByteBuffer buff) throws IOException {
        int length = buff.remaining();
        int position = extendBytes(buff.remaining());
        ByteBuffer writeBuffer = getWritableBuffer();
        writeBuffer.position(position);
        sync(() -> writeBuffer.put(buff), position, length, 4);
        return keyTableIdx.addBlock(position, length);
    }

    /**
     * Set an existing index to the buffer data.
     * @param keyIdxEntry
     * @param buff
     * @throws IOException
     */
    private void setKey(BufferTableIdx.IdxEntry keyIdxEntry, ByteBuffer buff) throws IOException {
        Stack<Func> undo = new Stack<Func>();
        try (RangeLock lock = keyIdxEntry.lock()) {
            undo.add(() -> keyIdxEntry.setDeleted(true));
            keyIdxEntry.setLen(buff.remaining());
            ByteBuffer writeBuffer = getWritableBuffer();
            writeBuffer.position(keyIdxEntry.getOffset());
            sync(() -> writeBuffer.put(buff), keyIdxEntry.getOffset(), keyIdxEntry.getLen(), 4);
            keyIdxEntry.setDeleted(false);
        } catch (IOException e) {
            execQuietly(undo);
            throw e;
        }
    }

    private BufferTableIdx.IdxEntry createNewEntry(int idx, ByteBuffer buff) throws IOException {

        Stack<Func> undo = new Stack<Func>();

        BufferTableIdx.IdxEntry keyIdxEntry = null;
        try {
            keyIdxEntry = keyTableIdx.search(buff.remaining());
            final BufferTableIdx.IdxEntry deleteKey = keyIdxEntry;
            undo.push(() -> {
                deleteKey.setInitialized(true);
                deleteKey.setDeleted(true);
            });
        } catch (IOException e1) {
            // ignore an try to create a new one.
        }
        try {
            if (keyIdxEntry == null) {
                keyIdxEntry = add(buff);
                final BufferTableIdx.IdxEntry deleteKey = keyIdxEntry;
                undo.push(() -> {
                    deleteKey.setInitialized(true);
                    deleteKey.setDeleted(true);
                });
                idxTable.get(idx).setKeyIdx(keyIdxEntry.getBlock());
                keyIdxEntry.setInitialized(true);
            } else {
                setKey(keyIdxEntry, buff);
            }
            return keyIdxEntry;
        } catch (IOException e) {
            execQuietly(undo);
            throw e;
        }

    }

    public void set(int idx, ByteBuffer buff) throws IOException {
        IdxMap.MapEntry mapEntry = idxTable.get(idx);
        if (mapEntry.isInitialized()) {
            // we are reusing the key so see if the buffer fits.
            BufferTableIdx.IdxEntry keyIdxEntry = keyTableIdx.get(mapEntry.getKeyIdx());
            if (keyIdxEntry.getAlloc() > buff.remaining()) {
                setKey(keyIdxEntry, buff);
            } else {
                // buffer did not fit so we need a new one and we need to free the old one.
                BufferTableIdx.IdxEntry newKeyIdxEntry = createNewEntry(idx, buff);
                mapEntry.setKeyIdx(newKeyIdxEntry.getBlock());
                keyIdxEntry.setDeleted(true);
            }
        } else {
            // this is a new key so just write it.
            BufferTableIdx.IdxEntry keyIdxEntry = createNewEntry(idx, buff);
            mapEntry.setKeyIdx(keyIdxEntry.getBlock());
        }
    }

    public void delete(int idx) throws IOException {
        IdxMap.MapEntry mapEntry = idxTable.get(idx);
        if (mapEntry.isInitialized()) {
            BufferTableIdx.IdxEntry keyIdx = keyTableIdx.get(mapEntry.getKeyIdx());
            keyIdx.setDeleted(true);
        }
    }
}
