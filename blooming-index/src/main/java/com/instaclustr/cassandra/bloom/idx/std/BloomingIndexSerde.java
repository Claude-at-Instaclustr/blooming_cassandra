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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.IndexKey;

/**
 * Handles all IO to the index table.
 *
 */
public class BloomingIndexSerde {

    private static final Logger logger = LoggerFactory.getLogger(BloomingIndexSerde.class);

    /**
     * The index table.
     */
    private final ColumnFamilyStore indexCfs;

    /**
     * Construct the BloomingIndex serializer/deserializer.
     *
     * <p>Defines the index table ColumnFamilyStore</p>
     *
     * @param baseCfs
     * @param indexMetadata
     */
    public BloomingIndexSerde(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
        TableMetadata baseCfsMetadata = baseCfs.metadata();
        TableMetadata.Builder builder = TableMetadata
                .builder(baseCfsMetadata.keyspace, baseCfsMetadata.indexTableName(indexMetadata), baseCfsMetadata.id)
                .kind(TableMetadata.Kind.INDEX).partitioner(new LocalPartitioner(LongType.instance))
                .addPartitionKeyColumn("pos", Int32Type.instance).addPartitionKeyColumn("code", Int32Type.instance)
                .addClusteringColumn("dataKey", BytesType.instance);

        TableMetadataRef tableRef = TableMetadataRef
                .forOfflineTools(builder.build().updateIndexTableMetadata(baseCfsMetadata.params));

        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace, tableRef.name, tableRef,
                baseCfs.getTracker().loadsstables);
    }

    /**
     * Gets the ClusteringComparator for the index table.
     * @return the ClusteringComparator for the index table.
     */
    private ClusteringComparator getIndexComparator() {
        return indexCfs.metadata().comparator;
    }

    /**
     * Gets the table for the index.
     * @return the backing table for the index.
     */
    public ColumnFamilyStore getBackingTable() {
        logger.trace("getBackingTable -- returning {}", indexCfs);
        return indexCfs;
    }

    /**
     * Calculates the estimated number of rows given the shape of the filters coming into
     * the index.
     *
     * @param entriesPerRow The number of entries per row.
     * @return <ul>
     * <li>  -1 : if entriesPerRow is <= 0.0 and there are items in the index.</li>
     * <li>   0 : if there are no entries in the index</li>
     * <li>other: The estimated number of base table rows represented in the index.</li>
     * </ul>
     */
    public long getEstimatedResultRows(double entriesPerRow) {
        logger.debug("getEstimatedResultRows( {} )", entriesPerRow);
        logger.debug( "indexCfs estimateKeys {}", indexCfs.estimateKeys());
        logger.debug( "indexCfs getMeanPartitionSize {}", indexCfs.getMeanPartitionSize());
        logger.debug( "indexCfs getMeanRowCount {}", indexCfs.getMeanRowCount());


        long entries = indexCfs.estimateKeys();
        if (entries == 0) {
            logger.debug("getEstimatedResultRows - No data in index, returning 0");
            return 0;
        }
        if (entriesPerRow <= 0.0) {
            return -1;
        }
        return Math.round(entries / entriesPerRow);
    }

    /**
     * Truncates the index at the specified location.
     * @param truncatedAt the location to truncate the table at.
     */
    public void truncate(long truncatedAt) {
        indexCfs.discardSSTables(truncatedAt);
    }

    /**
     * Reloads the index table.
     */
    public void reload() {
        indexCfs.reload();
    }

    /**
     * Forces a blocking flush on the index table.
     */
    public void forceBlockingFlush() {
        indexCfs.forceBlockingFlush();
    }

    /**
     * Gets the Decorated key for the index table.
     * @param value the ByteBuffer that contains the undecorated key.
     * @return the Decorated key for the index table.
     */
    private DecoratedKey getIndexKeyFor(ByteBuffer value) {
        return indexCfs.decorateKey(value);
    }

    /**
     * Constructs the clustering for an entry in the index table based on values from the base data.
     * The clustering columns in the index table encode the values required to retrieve the correct data
     * from the base table and varies depending on the kind of the indexed column.  Used whenever a row
     * in the index table is written or deleted.
     * @param rowKey the key from the row in the base table being indexed.
     * @param clustering the clustering from the row in the base table being indexed.
     * @return a clustering to be inserted into the index table.
     */
    private <T> Clustering<?> buildIndexClustering(DecoratedKey rowKey, Clustering<T> clustering) {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(rowKey.getKey());
        for (int i = 0; i < clustering.size(); i++)
            builder.add(clustering.get(i), clustering.accessor());
        return builder.build();
    }

    /**
     * Inserts a new entry into the index.
     * @param indexKey The key for the index.
     * @param rowKey the key for the row being indexed.
     * @param clustering the clustering for the row being indexed.
     * @param info the liveness of the primary key columns of the index row
     * @param ctx the write context to write with.
     */
    public void insert(IndexKey indexKey, DecoratedKey rowKey, Clustering<?> clustering, LivenessInfo info,
            WriteContext ctx) {
        logger.trace("Inserting {}", indexKey);
        Clustering<?> indexCluster = buildIndexClustering(rowKey, clustering);

        DecoratedKey valueKey = getIndexKeyFor(indexKey.asKey());
        Row row = BTreeRow.noCellLiveRow(indexCluster, info);
        PartitionUpdate upd = partitionUpdate(valueKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
        logger.trace("Inserted entry into index for value {}", valueKey);
    }

    /**
     * Deletes an entry from the index table.
     *
     * @param indexKey The index key to delete.
     * @param rowKey the key for the row being indexed.
     * @param clustering the clustering for the row being indexed.
     * @param deletedAt the time when the row was deleted
     * @param ctx the write context to write with.
     */
    public void delete(IndexKey indexKey, DecoratedKey rowKey, Clustering<?> clustering, DeletionTime deletedAt,
            WriteContext ctx) {
        logger.trace("Deleting {}", indexKey);
        DecoratedKey valueKey = getIndexKeyFor(indexKey.asKey());
        Clustering<?> indexClustering = buildIndexClustering(rowKey, clustering);
        Row row = BTreeRow.emptyDeletedRow(indexClustering, Row.Deletion.regular(deletedAt));
        PartitionUpdate upd = partitionUpdate(valueKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
        logger.trace("Removed index entry for value {}", valueKey);
    }

    /**
     * Creates a partition update on the index table.
     * @param valueKey the key for the index.
     * @param row  the row for the index.
     * @return the Partition update.
     */
    private PartitionUpdate partitionUpdate(DecoratedKey valueKey, Row row) {
        return PartitionUpdate.singleRowUpdate(indexCfs.metadata(), valueKey, row);
    }

    /**
     * Invalidates the index so that it will no longer be used.
     */
    public void invalidate() {
        // interrupt in-progress compactions
        Collection<ColumnFamilyStore> cfss = Collections.singleton(indexCfs);
        CompactionManager.instance.interruptCompactionForCFs(cfss, (sstable) -> true, true);
        CompactionManager.instance.waitForCessation(cfss, (sstable) -> true);
        Keyspace.writeOrder.awaitNewBarrier();
        indexCfs.forceBlockingFlush();
        indexCfs.readOrdering.awaitNewBarrier();
        indexCfs.invalidate();
    }

    /**
     * Reads the index table for all entries with the IndexKey.
     * @param indexKey the key to locate.
     * @param nowInSec The time of the query.
     * @param executionController the execution controller to use.
     * @return the UnfilteredRowIterator containing all matching entries.
     */
    public UnfilteredRowIterator read(IndexKey indexKey, int nowInSec, ReadExecutionController executionController) {
        TableMetadata indexMetadata = indexCfs.metadata();
        DecoratedKey valueKey = getIndexKeyFor(indexKey.asKey());
        return SinglePartitionReadCommand.fullPartitionRead(indexMetadata, nowInSec, valueKey)
                .queryMemtableAndDisk(indexCfs, executionController.indexReadController());

    }

    /**
     * Reads the index table for all entries with the IndexKey.
     * @param indexKey the key to locate.
     * @param nowInSec The time of the query.
     * @param executionController the execution controller to use.
     * @return the UnfilteredRowIterator containing all matching entries.
     */
    public UnfilteredRowIterator read(IndexKey indexKey, int nowInSec, DecoratedKey rowKey, Clustering<?> clustering) {
        TableMetadata indexMetadata = indexCfs.metadata();
        DecoratedKey valueKey = getIndexKeyFor(indexKey.asKey());
        SinglePartitionReadCommand readCommand = SinglePartitionReadCommand.create(indexMetadata, nowInSec, valueKey,
                buildIndexClustering(rowKey, clustering));
        try (ReadExecutionController readExecutionController = readCommand.executionController()) {
            return readCommand.queryMemtableAndDisk(indexCfs, readExecutionController);
        }
    }
}
