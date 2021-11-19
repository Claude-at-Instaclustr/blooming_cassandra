package com.instaclustr.cassandra.bloom.idx.std;

import java.util.Iterator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index.Indexer;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import com.instaclustr.cassandra.bloom.idx.IndexKey;

public class BloomingIndexer implements Indexer {

    private final DecoratedKey key;
    private final BloomingIndexSerde serde;
    private final ColumnMetadata indexedColumn;
    private final int nowInSec;
    private final WriteContext ctx;
    private final IndexTransaction.Type transactionType;

    public BloomingIndexer(final DecoratedKey key, final BloomingIndexSerde serde, final ColumnMetadata indexedColumn,
            final int nowInSec, final WriteContext ctx, final IndexTransaction.Type transactionType) {
        this.key = key;
        this.serde = serde;
        this.indexedColumn = indexedColumn;
        this.nowInSec = nowInSec;
        this.ctx = ctx;
        this.transactionType = transactionType;
    }

    @Override
    public void begin() {
    }

    @Override
    public void partitionDelete(DeletionTime deletionTime) {

    }

    @Override
    public void rangeTombstone(RangeTombstone tombstone) {
    }

    @Override
    public void insertRow(Row row) {
        insertRow(row, null);
    }

    private void insertRow(Row row, Iterator<IndexKey> iter) {
        if (row.isStatic()) {
            return;
        }

        Cell<?> cell = row.getCell(indexedColumn);
        if (cell == null || !cell.isLive(nowInSec)) {
            return;
        }
        Clustering<?> clustering = row.clustering();
        LivenessInfo info = LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime());

        insert(iter == null ? BFUtils.getIndexKeys(cell.buffer()) : iter, clustering, info);
    }

    private void insert(Iterator<IndexKey> rows, Clustering<?> clustering, LivenessInfo info) {
        while (rows.hasNext()) {
            serde.insert(rows.next(), key, clustering, info, ctx);
        }
    }

    @Override
    public void updateRow(Row oldRowData, Row newRowData) {
        if (newRowData.isStatic()) {
            if (!oldRowData.isStatic()) {
                removeRow(oldRowData);
            }
            return;
        }

        byte[] oldBytes = BFUtils.extractCodes(oldRowData.getCell(indexedColumn).buffer());
        byte[] newBytes = BFUtils.extractCodes(newRowData.getCell(indexedColumn).buffer());
        int limit = oldBytes.length > newBytes.length ? oldBytes.length : newBytes.length;
        int min = oldBytes.length > newBytes.length ? newBytes.length : oldBytes.length;
        boolean changed = false;

        long[] changes = new long[BitMap.numberOfBuckets(limit)];
        for (int i = 0; i < min; i++) {
            if (oldBytes[i] != newBytes[i]) {
                BitMap.set(changes, i);
                changed = true;
            }
        }
        for (int i = min; i < limit; i++) {
            BitMap.set(changes, i);
            changed = true;
        }
        if (!changed) {
            return;
        }

        // remove any old rows that have changed.
        removeRow(oldRowData, BFUtils.getIndexKeys(oldBytes).filterKeep(key -> {
            return BitMap.contains(changes, key.getPosition());
        }));
        // insert any new rows that have changed
        insertRow(newRowData, BFUtils.getIndexKeys(newBytes).filterKeep(key -> {
            return BitMap.contains(changes, key.getPosition());
        }));

    }

    @Override
    public void removeRow(Row row) {
        removeRow(row, null);
    }

    private void removeRow(Row row, Iterator<IndexKey> iter) {
        if (row.isStatic())
            return;

        Cell<?> cell = row.getCell(indexedColumn);
        if (cell == null || !cell.isLive(nowInSec)) {
            return;
        }
        Clustering<?> clustering = row.clustering();
        DeletionTime deletedAt = new DeletionTime(cell.timestamp(), nowInSec);

        remove(iter == null ? BFUtils.getIndexKeys(cell.buffer()) : iter, clustering, deletedAt);

    }

    private void remove(Iterator<IndexKey> rows, Clustering<?> clustering, DeletionTime deletedAt) {
        while (rows.hasNext()) {
            serde.delete(rows.next(), key, clustering, deletedAt, ctx);
        }
    }

    @Override
    public void finish() {
        // TODO Auto-generated method stub

    }

}
