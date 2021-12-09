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
package com.instaclustr.cassandra.bloom.idx.std;

import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index.Indexer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.CountingFilter;
import com.instaclustr.iterator.util.ExtendedIterator;
import com.instaclustr.iterator.util.WrappedIterator;

/**
 * Performs the index updating for inserting or removing a single row from the base table.
 *
 */
public class BloomingIndexer implements Indexer {

    private static final Logger logger = LoggerFactory.getLogger(BloomingIndexer.class);

    /**
     * The key for the base table.
     */
    private final DecoratedKey key;
    /**
     * The serde to read/write the index table.
     */
    private final BloomingIndexSerde serde;
    /**
     * The column in the base table that is indexed
     */
    private final ColumnMetadata indexedColumn;
    /**
     * The time this operation was started.
     */
    private final int nowInSec;
    /**
     * The context use for writing.
     */
    private final WriteContext ctx;
    /**
     * The base data table
     */
    private final ColumnFamilyStore baseCfs;

    /**
     * Constructor
     * @param key The key for the base table.
     * @param baseCfs The base data table
     * @param serde The serde to read/write the index table.
     * @param indexedColumn The time this operation was started.
     * @param nowInSec The time this operation was started.
     * @param ctx The context use for writing.
     */
    public BloomingIndexer(final DecoratedKey key, final ColumnFamilyStore baseCfs, final BloomingIndexSerde serde,
            final ColumnMetadata indexedColumn, final int nowInSec, final WriteContext ctx) {
        this.key = key;
        this.baseCfs = baseCfs;
        this.serde = serde;
        this.indexedColumn = indexedColumn;
        this.nowInSec = nowInSec;
        this.ctx = ctx;

    }

    @Override
    public void begin() {
        logger.trace("begin");
    }

    @Override
    public void partitionDelete(DeletionTime deletionTime) {
        logger.warn("partitionDelete -- Not Implemented");
    }

    @Override
    public void rangeTombstone(RangeTombstone tombstone) {
        logger.warn("rangeTombstone -- Not Implemented");
    }

    @Override
    public void insertRow(Row row) {
        logger.trace("insertRow");
        /*
         * single updates to the key only produce insert statements -- no deletes we
         * have to verify if there is already a record and read the existing bloom
         * filter if so
         */

        Clustering<?> clustering = row.clustering();
        boolean insertOnly = baseCfs.isEmpty();
        if (!insertOnly) {
            TableMetadata tableMetadata = baseCfs.metadata();
            ColumnFilter columnFilter = ColumnFilter.selectionBuilder().add(indexedColumn).build();

            SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(tableMetadata, nowInSec, key,
                    clustering);

            try (ReadExecutionController controller = dataCmd.executionController();
                    UnfilteredRowIterator rows = dataCmd.queryMemtableAndDisk(baseCfs, controller)) {
                insertOnly = rows.isEmpty() || !rows.columns().contains(indexedColumn);
                if (!insertOnly) {
                    Iterator<Row> rowIter = WrappedIterator.create(rows).filterKeep(Unfiltered::isRow).mapWith(u -> {
                        return ((Row) u).filter(columnFilter, tableMetadata);
                    });

                    if (rowIter.hasNext()) {
                        updateRow(rowIter.next(), row);
                    } else {
                        insertOnly = true;
                    }
                }
            }
        }

        if (insertOnly) {
            insertRow(row, null);
        }
    }

    /**
     * Inserts multiple rows into the index.
     * <p>If the {@code keys} parameter is null, The keys are extracted from the
     * indexed column in the row.</p>
     * @param row the Row from the base table that is being inserted.
     * @param keys The list of keys to insert. May be {@code null}.
     */
    private void insertRow(Row row, ExtendedIterator<IndexKey> keys) {
        if (row.isStatic()) {
            return;
        }

        Cell<?> cell = row.getCell(indexedColumn);
        if (cell == null || !cell.isLive(nowInSec)) {
            return;
        }
        Clustering<?> clustering = row.clustering();
        LivenessInfo info = LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime());
        Consumer<IndexKey> operation = (r) -> serde.insert(r, key, clustering, info, ctx);
        update(operation, keys == null ? BFUtils.getIndexKeys(cell.buffer()) : keys, "Inserted");
    }

    /**
     * Inserts or deletes multiple rows into the index table.
     * @param operation An IndexKey consumer that calls serde to perform the operation.
     * @param rows The collection of keys to insert.
     * @param op literal "Inserted" or "Deleted" for logging purposes
     * @throws Exception
     */
    private void update(Consumer<IndexKey> operation, ExtendedIterator<IndexKey> rows, String op) {
        CountingFilter<IndexKey> counting = null;
        if (logger.isDebugEnabled()) {
            counting = new CountingFilter<IndexKey>();
            rows.filterKeep(counting);
        }

        rows.forEach(operation);

        if (logger.isDebugEnabled()) {
            logger.debug("{} {} keys", op, counting.getCount());
        }

    }

    /**
     * Extracts the bytes from a Bloom filter cell.
     * @param row the Cell to extract the data from (May be null).
     * @return a byte array (May be empty).
     */
    private byte[] extractBytes(Row row) {
        Cell<?> cell = row.getCell(indexedColumn);
        if (cell != null) {
            return BFUtils.extractCodes(cell.buffer());
        }
        return new byte[0];
    }

    @Override
    public void updateRow(Row oldRowData, Row newRowData) {
        logger.trace("updateRow");
        if (newRowData.isStatic()) {
            if (!oldRowData.isStatic()) {
                removeRow(oldRowData);
            }
            return;
        }

        byte[] oldBytes = extractBytes(oldRowData);
        byte[] newBytes = extractBytes(newRowData);
        /*
         * verify oldBytes were actually indexed
         */
        try (ExtendedIterator<IndexKey> keyIter = BFUtils.getIndexKeys(oldBytes)) {
            if (keyIter.hasNext()) {
                boolean doSingle = false;
                try (UnfilteredRowIterator rowIter = serde.read(keyIter.next(), nowInSec, key,
                        oldRowData.clustering())) {
                    doSingle = !rowIter.hasNext();
                }
                if (doSingle) {
                    insertRow(newRowData, BFUtils.getIndexKeys(newBytes));
                    return;
                }

            }
        }

        if (oldBytes.length == 0) {
            if (newBytes.length != 0) {
                insertRow(newRowData, BFUtils.getIndexKeys(newBytes));
            }
            return;
        } else if (newBytes.length == 0) {
            removeRow(oldRowData, BFUtils.getIndexKeys(oldBytes));
            return;
        }
        /*
         * Calculate a diff by comparing the bytes. If a byte chagnes set a bit in the
         * "changes" BitMap array. Later use the changed bits to determine which
         * IndexKeys to remove or add.
         */
        int limit = oldBytes.length > newBytes.length ? oldBytes.length : newBytes.length;
        int min = oldBytes.length > newBytes.length ? newBytes.length : oldBytes.length;
        boolean changed = false;

        long[] changes = new long[BitMap.numberOfBitMaps(limit)];
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
        logger.trace("removeRow");
        removeRow(row, null);
    }

    /**
     * Removes multiple rows from the index.
     * <p>If the {@code keys} parameter is null, The keys are extracted from the
     * indexed column in the row.</p>
     * @param row the Row from the base table that is being removed.
     * @param keys The list of keys to remove. May be {@code null}.
     */
    private void removeRow(Row row, ExtendedIterator<IndexKey> keys) {
        if (row.isStatic())
            return;

        Cell<?> cell = row.getCell(indexedColumn);
        if (cell == null || !cell.isLive(nowInSec)) {
            return;
        }
        Clustering<?> clustering = row.clustering();
        DeletionTime deletedAt = new DeletionTime(cell.timestamp(), nowInSec);
        Consumer<IndexKey> operation = (r) -> serde.delete(r, key, clustering, deletedAt, ctx);
        update(operation, keys == null ? BFUtils.getIndexKeys(cell.buffer()) : keys, "Deleted");

    }

    @Override
    public void finish() {
        logger.trace("finish");
    }

}
