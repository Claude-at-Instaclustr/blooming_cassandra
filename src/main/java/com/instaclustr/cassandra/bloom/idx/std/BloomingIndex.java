package com.instaclustr.cassandra.bloom.idx.std;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.IndexMetadata;

public class BloomingIndex implements Index {

    public BloomingIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
        super( baseCfs, indexDef);
    }

    @Override
    protected CBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey, ClusteringPrefix prefix, CellPath path) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isStale(Row row, ByteBuffer indexValue, int nowInSec) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected ByteBuffer getIndexedValue(ByteBuffer partitionKey, Clustering clustering, CellPath path,
            ByteBuffer cellValue) {
        // TODO Auto-generated method stub
        return null;
    }

}
