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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

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
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.FilteredRows;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BufferTable;

/**
 * Handles all IO to the index table.
 *
 */
public class FlatBloomingIndexSerde {

    public static final Logger logger = LoggerFactory.getLogger(FlatBloomingIndexSerde.class);

    /**
     * We use this as the block size for the key buffer table.
     */
    private static final int AVERAGE_KEY_SIZE = 1024;

    /**
     * The index table.
     */
    private final ColumnFamilyStore indexCfs;

    private final FlatBloofi flatBloofi;
    private final BufferTable keyTable;

    /**
     * Construct the BloomingIndex serializer/deserializer.
     *
     * <p>Defines the index table ColumnFamilyStore</p>
     *
     * @param baseCfs
     * @param indexMetadata
     */
    public FlatBloomingIndexSerde(File dir, ColumnFamilyStore baseCfs, IndexMetadata indexMetadata, int numberOfBits) {

        TableMetadata baseCfsMetadata = baseCfs.metadata();
        TableMetadata.Builder builder = TableMetadata
                .builder(baseCfsMetadata.keyspace, baseCfsMetadata.indexTableName(indexMetadata), baseCfsMetadata.id)
                .kind(TableMetadata.Kind.INDEX).partitioner(new LocalPartitioner(BytesType.instance))
                .addPartitionKeyColumn("dataKey", BytesType.instance).addClusteringColumn("idx", Int32Type.instance);

        TableMetadataRef tableRef = TableMetadataRef
                .forOfflineTools(builder.build().updateIndexTableMetadata(baseCfsMetadata.params));

        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace, tableRef.name, tableRef,
                baseCfs.getTracker().loadsstables);

        try {
            flatBloofi = new FlatBloofi(new File(dir, "data"), new File(dir, "busy"), numberOfBits);
        } catch (IOException e) {
            throw new RuntimeException("Unable to crate FlatBloofi", e);
        }

        try {
            keyTable = new BufferTable(new File(dir, "keys"), AVERAGE_KEY_SIZE);
        } catch (IOException e) {
            BaseTable.closeQuietly(flatBloofi);
            throw new RuntimeException("Unable to crate keyTable", e);
        }

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
     * Returns a count of the number of items in the index.
     * @return the number of items in the index
     * @throws IOException on IO Error.
     */
    public int count() throws IOException {
        return flatBloofi.count();
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
    private <T> DecoratedKey buildIndexKey(DecoratedKey rowKey, Clustering<T> clustering) {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(rowKey.getKey());
        for (int i = 0; i < clustering.size(); i++)
            builder.add(clustering.get(i), clustering.accessor());
        return getIndexKeyFor(builder.build().bufferAt(0));
    }

    private Clustering<?> buildClustering(int idx) {
        byte[] bytes = new byte[Integer.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(idx);
        buffer.flip();
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(buffer);
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
    public void insert(int nowInSec, DecoratedKey rowKey, Clustering<?> clustering, LivenessInfo info, WriteContext ctx,
            ByteBuffer bloomFilter) {
        logger.trace("Inserting {}", rowKey);

        int idx = read(nowInSec, rowKey, clustering);
        try {
            if (idx < 0) {
                // new record
                idx = flatBloofi.add(bloomFilter);
                DecoratedKey valueKey = buildIndexKey(rowKey, clustering);
                Clustering<?> indexClustering = buildClustering(idx);
                Row row = BTreeRow.noCellLiveRow(indexClustering, info);
                PartitionUpdate upd = partitionUpdate(valueKey, row);
                indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
                logger.trace("Inserted entry into index as {} for row {}", idx, rowKey);
            } else {
                flatBloofi.update(idx, bloomFilter);
                logger.trace("Updated entry into index as {} for row {}", idx, rowKey);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to read from index", e);
        }

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
    public void delete(DecoratedKey rowKey, Clustering<?> clustering, DeletionTime deletedAt, WriteContext ctx) {
        logger.trace("Deleting {}", rowKey);
        int idx = read(deletedAt.localDeletionTime(), rowKey, clustering);
        if (idx != BufferTable.UNSET) {
            try {
                flatBloofi.delete(idx);
                keyTable.delete(idx);
            } catch (IOException e) {
                logger.warn(String.format("Error attempting to delete %s (%s}", idx, rowKey), e);
            }
        }

        DecoratedKey valueKey = buildIndexKey(rowKey, clustering);
        Clustering<?> indexClustering = buildClustering(idx);
        Row row = BTreeRow.emptyDeletedRow(indexClustering, Row.Deletion.regular(deletedAt));
        PartitionUpdate upd = partitionUpdate(valueKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
        logger.trace("Removed index entry for value {}", valueKey);
    }

    public void search(Consumer<ByteBuffer> consumer, ByteBuffer bloomFilter) throws IOException {
        IntConsumer intConsumer = new IntConsumer() {

            @Override
            public void accept(int idx) {
                try {
                    consumer.accept(keyTable.get(idx));
                } catch (IOException e) {
                    logger.warn(String.format("Error retrieve key for index %s", idx), e);
                }
            }

        };
        flatBloofi.search(intConsumer, bloomFilter);

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

    // /**
    // * Reads the index table for all entries with the IndexKey.
    // * @param indexKey the key to locate.
    // * @param nowInSec The time of the query.
    // * @param executionController the execution controller to use.
    // * @return the UnfilteredRowIterator containing all matching entries.
    // */
    // public int read(IndexKey indexKey, int nowInSec, ReadExecutionController
    // executionController) {
    // TableMetadata indexMetadata = indexCfs.metadata();
    // DecoratedKey valueKey = getIndexKeyFor(indexKey.asKey());
    // return SinglePartitionReadCommand.fullPartitionRead(indexMetadata, nowInSec,
    // valueKey)
    // .queryMemtableAndDisk(indexCfs, executionController.indexReadController());
    //
    // }

    /**
     * Reads the index table for all entries with the IndexKey.
     * @param indexKey the key to locate.
     * @param nowInSec The time of the query.
     * @param executionController the execution controller to use.
     * @return the UnfilteredRowIterator containing all matching entries.
     */
    private int read(int nowInSec, DecoratedKey rowKey, Clustering<?> clustering) {
        TableMetadata indexMetadata = indexCfs.metadata();
        DecoratedKey valueKey = buildIndexKey(rowKey, clustering);

        SinglePartitionReadCommand readCommand = SinglePartitionReadCommand.create(indexMetadata, nowInSec, valueKey,
                Clustering.EMPTY);
        try (ReadExecutionController readExecutionController = readCommand.executionController();
                RowIterator rowIter = FilteredRows
                        .filter(readCommand.queryMemtableAndDisk(indexCfs, readExecutionController), nowInSec)) {

            if (!rowIter.hasNext()) {
                return BufferTable.UNSET;
            }

            int result = rowIter.next().clustering().bufferAt(0).asIntBuffer().get(0);
            if (rowIter.hasNext()) {
                throw new IllegalStateException(String.format("Too many results for %s", valueKey));
            }
            return result;
        }
    }
}
