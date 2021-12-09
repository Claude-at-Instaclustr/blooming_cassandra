package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import com.instaclustr.cassandra.bloom.idx.mem.tables.IdxTable.IdxEntry;

public class KeyTable extends AbstractTable {

    private static final int UNSET = -1;

    private final IdxTable idxTable;

    /**
     * Header strucutr
     *  int firstDeleted
     *  int lastDeleted
     */
    private static final int FIRST_DELETED = 0;
    private static final int LAST_DELETED = 1;
    private static final int HEADER_SIZE = 2 * Integer.BYTES;

    private static final int DELETED_SIZE = 2 * Integer.BYTES;
    private class Deleted {
        int allocated;
        int nextDeleted;
    }


    @Override
    public void close() throws IOException {
        closeQuietly( idxTable );
        super.close();
    }

    @Override
    public String toString() {
        return "KeyTable: "+super.toString();
    }

    public KeyTable(File file) throws IOException {
        super(file);
        if (getFileSize()==0) {
            IntBuffer buffer = getWritableIntBuffer();
            buffer.put( UNSET );
            buffer.put( UNSET );
        }
        File idxFile = new File(file.getParentFile(), file.getName()+"_idx" );
        idxTable = new IdxTable( idxFile );
    }

    private void setDeleted( ByteBuffer buffer, IdxEntry idxEntry ) {
        int firstDeleted = buffer.getInt(FIRST_DELETED);
        int lastDeleted = buffer.getInt(LAST_DELETED);
        buffer.position( idxEntry.getPos() );
        buffer.putInt( idxEntry.getAlloc() );
        buffer.putInt( UNSET );
        if (firstDeleted == UNSET) {
            buffer.putInt(0, idxEntry.getPos() );
        }
        if (lastDeleted != UNSET ) {
            buffer.putInt( lastDeleted+Integer.BYTES, idxEntry.getPos() );
        }
        buffer.putInt( Integer.BYTES, idxEntry.getPos() );
    }

    private void findDeleted( ByteBuffer buffer, IdxEntry idxEntry ) {
        int pos = buffer.get(FIRST_DELETED);
        int lastPos = UNSET;
        while (pos != UNSET) {
            if (buffer.getInt(pos) >= idxEntry.getLen()) {
                if (lastPos != UNSET) {
                    buffer.put(lastPos+Integer.BYTES, buffer.get(pos+Integer.BYTES));
                }
                idxEntry.setAlloc( buffer.getInt(pos) );
                idxEntry.setPos( pos );
                buffer.position( pos );
                return;
            }
            lastPos = pos;
            pos = buffer.getInt( pos+Integer.BYTES);
        }
    }

    public ByteBuffer get( int idx) throws IOException {
        IdxEntry idxEntry = idxTable.get(idx);
        ByteBuffer buff = getBuffer();
        int pos = idxEntry.getPos();
        buff.position( pos );
        buff.limit( pos+idxEntry.getLen() );
        return buff;
    }

    public void set( int idx, ByteBuffer buff ) throws IOException {
        IdxEntry idxEntry = idxTable.get(idx);
        ByteBuffer table = getWritableBuffer();
        if (idxEntry.getAlloc() < buff.remaining() ) {
            setDeleted( table, idxEntry );
            idxEntry.setPos( UNSET );
        } else {
            table.position( idxEntry.getPos() );
            table.put( buff );
        }
        // Write a new one
        if (idxEntry.getPos() == UNSET ) {
            findDeleted( table, idxEntry );
            if (idxEntry.getPos() == UNSET ) {
                idxEntry.setPos( table.capacity() );
                idxEntry.setAlloc( buff.remaining() );
            }
        }
        table.position( idxEntry.getPos() );
        table.put( buff );
    }

    public void delete( int idx ) throws IOException {
        IdxEntry idxEntry = idxTable.get(idx);
        ByteBuffer table = getWritableBuffer();
        setDeleted( table, idxEntry );
    }
}
