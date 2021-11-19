package com.instaclustr.cassandra.bloom.idx.std;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.IndexKey;

public class BloomingIndexSerde {

    private static final Logger logger = LoggerFactory.getLogger(BloomingIndexSerde.class);

    private final ColumnFamilyStore indexCfs;

    public BloomingIndexSerde(ColumnFamilyStore indexCfs) {
        this.indexCfs = indexCfs;
    }

    public ClusteringComparator getIndexComparator() {
        return indexCfs.metadata().comparator;
    }

    public Optional<ColumnFamilyStore> getBackingTable() {
        return Optional.of(indexCfs);
    }

    public long getEstimatedResultRows() {
        // FIXME this must be adjusted by estimated number of items
        return indexCfs.getMeanRowCount();
    }

    public void truncate(long truncatedAt) {
        indexCfs.discardSSTables(truncatedAt);
    }

    public void reload() {
        indexCfs.reload();
    }

    public void forceBlockingFlush() {
        indexCfs.forceBlockingFlush();
    }

    public DecoratedKey getIndexKeyFor(ByteBuffer value) {
        return indexCfs.decorateKey(value);
    }

    /**
     * Used to construct an the clustering for an entry in the index table based on values from the base data.
     * The clustering columns in the index table encode the values required to retrieve the correct data from the base
     * table and varies depending on the kind of the indexed column. See indexCfsMetadata for more details
     * Used whenever a row in the index table is written or deleted.
     * @param partitionKey from the base data being indexed
     * @param prefix from the base data being indexed
     * @param cell from the base data being indexed
     * @return a clustering prefix to be used to insert into the index table
     */
    private <T> Clustering<?> buildIndexClustering(DecoratedKey rowKey, Clustering<T> clustering) {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(rowKey.getKey());
        for (int i = 0; i < clustering.size(); i++)
            builder.add(clustering.get(i), clustering.accessor());
        return builder.build();
    }

    /**
     * Called when adding a new entry to the index
     */

    public void insert(IndexKey indexKey, DecoratedKey rowKey, Clustering<?> clustering, LivenessInfo info,
            WriteContext ctx) {
        DecoratedKey valueKey = getIndexKeyFor(indexKey.asKey());
        Row row = BTreeRow.noCellLiveRow(buildIndexClustering(rowKey, clustering), info);

        PartitionUpdate upd = partitionUpdate(valueKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
        logger.trace("Inserted entry into index for value {}", valueKey);
    }

    /**
     * Called when deleting entries on non-primary key columns
     */
    public void delete(IndexKey indexKey, DecoratedKey rowKey, Clustering<?> clustering, DeletionTime deletedAt,
            WriteContext ctx) {
        DecoratedKey valueKey = getIndexKeyFor(indexKey.asKey());

        doDelete(valueKey, buildIndexClustering(rowKey, clustering), deletedAt, ctx);
    }

    private void doDelete(DecoratedKey indexKey, Clustering<?> indexClustering, DeletionTime deletion,
            WriteContext ctx) {
        Row row = BTreeRow.emptyDeletedRow(indexClustering, Row.Deletion.regular(deletion));
        PartitionUpdate upd = partitionUpdate(indexKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
        logger.trace("Removed index entry for value {}", indexKey);
    }

    private PartitionUpdate partitionUpdate(DecoratedKey valueKey, Row row) {
        return PartitionUpdate.singleRowUpdate(indexCfs.metadata(), valueKey, row);
    }

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

    public UnfilteredRowIterator read(IndexKey indexKey, ReadCommand command,
            ReadExecutionController executionController) {
        TableMetadata indexMetadata = indexCfs.metadata();
        DecoratedKey valueKey = getIndexKeyFor(indexKey.asKey());
        return SinglePartitionReadCommand.fullPartitionRead(indexMetadata, command.nowInSec(), valueKey)
                .queryMemtableAndDisk(indexCfs, executionController.indexReadController());

    }
}
