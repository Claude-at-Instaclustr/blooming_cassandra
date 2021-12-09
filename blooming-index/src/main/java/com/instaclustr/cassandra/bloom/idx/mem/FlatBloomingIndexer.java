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
package com.instaclustr.cassandra.bloom.idx.mem;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

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
import com.instaclustr.cassandra.bloom.idx.mem.tables.IdxTable;
import com.instaclustr.cassandra.bloom.idx.mem.tables.KeyTable;
import com.instaclustr.iterator.util.ExtendedIterator;
import com.instaclustr.iterator.util.WrappedIterator;

/**
 * Performs the index updating for inserting or removing a single row from the base table.
 *
 */
public class FlatBloomingIndexer implements Indexer {

    private static final Logger logger = LoggerFactory.getLogger(FlatBloomingIndexer.class);

    /**
     * The key for the base table.
     */
    private final DecoratedKey key;

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

    private final FlatBloomingIndexSerde serde;

    /**
     * Constructor
     * @param key The key for the base table.
     * @param baseCfs The base data table
     * @param serde The serde to read/write the index table.
     * @param indexedColumn The time this operation was started.
     * @param nowInSec The time this operation was started.
     * @param ctx The context use for writing.
     */
    public FlatBloomingIndexer(FlatBloomingIndexSerde serde, final DecoratedKey key, final ColumnFamilyStore baseCfs,
            final ColumnMetadata indexedColumn, final int nowInSec, final WriteContext ctx) {
        this.serde = serde;
        this.key = key;
        this.baseCfs = baseCfs;
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

        if (row.isStatic()) {
            return;
        }

        Cell<?> cell = row.getCell(indexedColumn);
        if (cell == null || !cell.isLive(nowInSec)) {
            return;
        }
        Clustering<?> clustering = row.clustering();
        LivenessInfo info = LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime());


        serde.insert(key, clustering, info, ctx, cell.buffer() );
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
        removeRow( oldRowData );
        insertRow( newRowData );
    }

    @Override
    public void removeRow(Row row) {

        if (row.isStatic())
            return;

        Cell<?> cell = row.getCell(indexedColumn);
        if (cell == null || !cell.isLive(nowInSec)) {
            return;
        }
        Clustering<?> clustering = row.clustering();
        DeletionTime deletedAt = new DeletionTime(cell.timestamp(), nowInSec);
        serde.delete( key, clustering, deletedAt, ctx);

    }

    @Override
    public void finish() {
        logger.trace("finish");
    }

}
