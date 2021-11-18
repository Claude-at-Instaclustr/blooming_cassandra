package com.instaclustr.cassandra.bloom.idx.std;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index.Indexer;

public class BFIndexer implements Indexer {

    public BFIndexer() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void begin() {
        // TODO Auto-generated method stub

    }

    @Override
    public void partitionDelete(DeletionTime deletionTime) {
        // TODO Auto-generated method stub

    }

    @Override
    public void rangeTombstone(RangeTombstone tombstone) {
        // TODO Auto-generated method stub

    }

    @Override
    public void insertRow(Row row) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateRow(Row oldRowData, Row newRowData) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeRow(Row row) {
        // TODO Auto-generated method stub

    }

    @Override
    public void finish() {
        // TODO Auto-generated method stub

    }

}
