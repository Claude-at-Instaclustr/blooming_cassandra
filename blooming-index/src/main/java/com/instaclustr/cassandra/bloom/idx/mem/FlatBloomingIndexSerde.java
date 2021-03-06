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
import java.nio.charset.StandardCharsets;
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
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.FilteredRows;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.OutputTimeoutException;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BufferTable;

/**
 * Handles all IO to the index tables.
 *
 */
public class FlatBloomingIndexSerde {

    public static final Logger logger = LoggerFactory.getLogger(FlatBloomingIndexSerde.class);

    /**
     * We use this as the block size for the key buffer table.
     */
    private static final int AVERAGE_KEY_SIZE = Long.BYTES;

    /**
     * The index table.
     */
    private final ColumnFamilyStore indexCfs;

    /**
     * The FlatBloofi we are using.
     */
    private final FlatBloofi flatBloofi;

    /**
     * The key table.
     */
    private final BufferTable keyTable;

    /**
     * The column metadata for the index column.
     */
    private final ColumnMetadata idxColumn;

    /**
     * Construct the FlatBloomingIndex serializer/deserializer.
     *
     * <p>Defines the index table ColumnFamilyStore</p>
     *
     * @param baseCfs the Base table ColumnFamilyStore.
     * @param indexMetadata the Index metadata.
     * @param numberofBits The number of bits in the Bloom filters.
     */
    public FlatBloomingIndexSerde(File dir, ColumnFamilyStore baseCfs, IndexMetadata indexMetadata, int numberOfBits) {

        ByteBuffer idxColumnName = ByteBuffer.wrap("idx".getBytes(StandardCharsets.UTF_8));
        TableMetadata baseCfsMetadata = baseCfs.metadata();
        TableMetadata.Builder builder = TableMetadata
                .builder(baseCfsMetadata.keyspace, baseCfsMetadata.indexTableName(indexMetadata), baseCfsMetadata.id)
                .kind(TableMetadata.Kind.INDEX).partitioner(new LocalPartitioner(BytesType.instance));

        for (ColumnMetadata partitionColumn : baseCfsMetadata.partitionKeyColumns()) {
            builder.addPartitionKeyColumn(partitionColumn.name, partitionColumn.type);
        }
        for (ColumnMetadata clusteringColumn : baseCfsMetadata.clusteringColumns()) {
            builder.addClusteringColumn(clusteringColumn.name, clusteringColumn.type);
        }
        builder.addRegularColumn("idx", Int32Type.instance);

        TableMetadataRef tableRef = TableMetadataRef
                .forOfflineTools(builder.build().updateIndexTableMetadata(baseCfsMetadata.params));

        idxColumn = tableRef.get().getColumn(idxColumnName);
        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace, tableRef.name, tableRef,
                baseCfs.getTracker().loadsstables);

        try {
            flatBloofi = new FlatBloofi(dir, numberOfBits);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create FlatBloofi", e);
        }

        try {
            keyTable = new BufferTable(new File(dir, "keys"), AVERAGE_KEY_SIZE);
        } catch (IOException e) {
            BaseTable.closeQuietly(flatBloofi);
            throw new RuntimeException("Unable to create keyTable", e);
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
     * Constructs the clustering for an entry in the index table based on values from the base data.
     * The clustering columns in the index table encode the values required to retrieve the correct data
     * from the base table and varies depending on the kind of the indexed column.  Used whenever a row
     * in the index table is written or deleted.
     * @param rowKey the key from the row in the base table being indexed.
     * @param clustering the clustering from the row in the base table being indexed.
     * @return a clustering to be inserted into the index table.
     */
    public <T> Clustering<?> buildIndexClustering(Clustering<T> clustering) {
        CBuilder builder = CBuilder.create(getIndexComparator());
        for (int i = 0; i < clustering.size(); i++)
            builder.add(clustering.get(i), clustering.accessor());
        return builder.build();
    }

    /**
     * Performs the insert into the index.
     * @param idx the Index id.
     * @param rowKey the Base table key for the row being inserted.
     * @param bloomFilter the Bloom filter being inserted.
     * @param clustering the Clustering columns for the Base table.
     * @param timestamp the time of the insert.
     * @param ctx the write context to write with.
     * @throws IOException on IO error.
     */
    private void doInsert(int idx, DecoratedKey rowKey, ByteBuffer bloomFilter, Clustering<?> clustering,
            long timestamp, WriteContext ctx) throws IOException {
        if (rowKey.getKey().remaining() == 0) {
            logger.error("Invalid key length for {}", rowKey);
            throw new IOException(String.format("Invalid key length for %s", rowKey) );
        }

        /* create new record */
        if (idx < 0) {
            idx = flatBloofi.add(bloomFilter);
            boolean loop = true;
            while (loop)
                try {
                    /* write the row key to the key Table */
                    keyTable.set(idx, rowKey.getKey());
                    loop = false;
                } catch (OutputTimeoutException e) {
                    logger.debug("Timeout trying to write keyTable entry for {}.  Trying again.", idx);
                } catch (IOException e) {
                    logger.warn("Error {} trying to write tkeyTable entry for {}", e, idx);
                    throw e;
                }
        }

        /* write the index entry */
        Clustering<?> indexClustering = buildIndexClustering(clustering);
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
        bb.putInt(idx);
        bb.flip();
        Cell<ByteBuffer> cell = BufferCell.live(idxColumn, timestamp, bb);
        Row btRow = BTreeRow.singleCellRow(indexClustering, cell);
        PartitionUpdate upd = partitionUpdate(convertToIndexKey(rowKey), btRow);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);

    }

    /**
     * Inserts a new entry into the index.  Will update an existing entry or create a new one as necessary.
     * @param nowInSec the time of the insert in seconds.
     * @param rowKey the Base table key for the row being inserted.
     * @param row the Row being inserted.
     * @param timestamp the time of the insert.
     * @param ctx the write context to write with.
     * @param bloomFilter the Bloom filter being inserted.
     * @throws IOException on IOError
     */
    public void insert(int nowInSec, DecoratedKey rowKey, Row row, long timestamp, WriteContext ctx,
            ByteBuffer bloomFilter) throws IOException {
        logger.debug("Inserting {}", row);
        int idx = read(nowInSec, rowKey, row.clustering());

        if (idx < 0) {
            flatBloofi.exec(() -> keyTable
                    .retryOnTimeout(() -> doInsert(idx, rowKey, bloomFilter, row.clustering(), timestamp, ctx)));
        } else {
            flatBloofi.exec(() -> keyTable.retryOnTimeout(() -> flatBloofi.update(idx, bloomFilter)));
        }

    }

    /**
     * Deletes an entry from the index.
     *
     * @param rowKey the Base table key for the row being deleted.
     * @param deletedAt the time when the row was deleted
     * @param ctx the write context to write with.
     */
    public void delete(DecoratedKey rowKey, Row row, DeletionTime deletedAt, WriteContext ctx) {
        logger.trace("Deleting {}", rowKey);
        int idx = read(deletedAt.localDeletionTime(), rowKey, row.clustering());
        if (idx != BufferTable.UNSET) {

            flatBloofi.exec(() -> keyTable.retryOnTimeout(() -> flatBloofi.delete(idx)));
            keyTable.exec(() -> keyTable.retryOnTimeout(() -> keyTable.delete(idx)));

        }

        Clustering<?> indexClustering = buildIndexClustering(row.clustering());
        Row btRow = BTreeRow.emptyDeletedRow(indexClustering, Row.Deletion.regular(deletedAt));
        PartitionUpdate upd = partitionUpdate(convertToIndexKey(rowKey), btRow);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
        logger.trace("Removed index entry for value {}", rowKey);
    }

    /**
     * Updates an entry from the index.  Will only perform update if the row is already indexed.
     *
     * @param rowKey the Base table key for the row being updated.
     * @param clustering the clustering for the row being indexed.
     * @param bloomFilter the Bloom filter being inserted.
     * @return {@code true} on success,{@code false} if no update occured.
     */
    public boolean update(DecoratedKey rowKey, Clustering<?> clustering, int nowInSec, ByteBuffer bloomFilter) {
        logger.debug("Updating {}", rowKey);
        int idx = read(nowInSec, rowKey, clustering);
        if (idx != BufferTable.UNSET) {
            try {
                flatBloofi.update(idx, bloomFilter);
                return true;
            } catch (IOException e) {
                logger.warn(String.format("Error attempting to update %s (%s}", idx, rowKey), e);
            }
        }
        return false;
    }

    /**
     * Searches for an entry in the index.  Will only perform update if the row is already indexed.
     *
     * @param consumer A consumer to accept the found Bloom filters.
     * @param bloomFilter the Bloom filter to search for.
     * @throws IOException on IO error.
     */
    public void search(Consumer<ByteBuffer> consumer, ByteBuffer bloomFilter) throws IOException {
        IntConsumer intConsumer = new IntConsumer() {

            @Override
            public void accept(int idx) {
                try {
                    ByteBuffer bb = keyTable.get(idx);
                    if (bb != null) {
                        consumer.accept(bb);
                    }
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
     *
     * This will delete data for the index.
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
        flatBloofi.drop();
        keyTable.drop();
    }

    /**
     * Convert a row key to an index key.
     * @param rowKey the Base table key for the row being indexed.
     * @return the Index table key for the row being indexed.
     */
    private DecoratedKey convertToIndexKey(DecoratedKey rowKey) {
        return indexCfs.decorateKey(rowKey.getKey());
    }

    /**
     * Reads the index table for the Bloom filter index associated with the Row key.
     * @param nowInSec the time of the read.
     * @param rowKey the Base table key for the row being read.
     * @param clustering the Clustering columns for the Base table.
     * @return index value or {@code BufferTable.UNSET} if not found
     * @see BufferTable#UNSET
     */
    private int read(int nowInSec, DecoratedKey rowKey, Clustering<?> clustering) {
        SinglePartitionReadCommand readCommand = SinglePartitionReadCommand.create(indexCfs.metadata(), nowInSec,
                convertToIndexKey(rowKey), clustering);
        try (ReadExecutionController readExecutionController = readCommand.executionController();
                RowIterator rowIter = FilteredRows
                        .filter(readCommand.queryMemtableAndDisk(indexCfs, readExecutionController), nowInSec)) {

            if (!rowIter.hasNext()) {
                return BufferTable.UNSET;
            }

            int result = rowIter.next().getCell(idxColumn).buffer().asIntBuffer().get();
            if (rowIter.hasNext()) {
                throw new IllegalStateException(String.format("Too many results for %s", rowKey));
            }
            return result;
        }
    }
}
