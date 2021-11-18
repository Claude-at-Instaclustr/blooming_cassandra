package com.instaclustr.cassandra.bloom.idx.std;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.Index.Indexer;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexFunctions;
import org.apache.cassandra.index.internal.CollatedViewIndexBuilder;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.index.internal.composites.CompositesSearcher;
import org.apache.cassandra.index.internal.composites.RegularColumnIndex;
import org.apache.cassandra.index.internal.keys.KeysSearcher;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class BloomingIndex implements Index {

    private static final Logger logger = LoggerFactory.getLogger(CassandraIndex.class);

    public final ColumnFamilyStore baseCfs;
    protected IndexMetadata metadata;
    protected ColumnFamilyStore indexCfs;
    protected ColumnMetadata indexedColumn;

    private static BitMapProducer createBitMapProducer( ByteBuffer buffer ) {

        return new BitMapProducer() {
            LongBuffer buff = buffer.asLongBuffer();
            @Override
            public void forEachBitMap(LongConsumer consumer) {
                while (buff.hasRemaining()) {
                    consumer.accept(buff.get());
                }
            }
        };
    }

    private static ExtendedIterator<Long> createLongIterator( ByteBuffer buffer ) {
        return WrappedIterator.create( new Iterator<Long>() {

        LongBuffer buff = buffer.asLongBuffer();

        @Override
        public boolean hasNext() {
            return buff.hasRemaining();
        }

        @Override
        public Long next() {
            if (hasNext()) {
                return buff.get();
            } else {
                throw new NoSuchElementException();
            }
        }
        }
                );
    }
    /**
     * Construct the TableMetadata for an index table, the clustering columns in the index table
     * vary dependent on the kind of the indexed value.
     * @param baseCfsMetadata
     * @param indexMetadata
     * @return
     */
    public static TableMetadata indexCfsMetadata(TableMetadata baseCfsMetadata, IndexMetadata indexMetadata)
    {
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfsMetadata, indexMetadata);



        TableMetadata.Builder builder =
            TableMetadata.builder(baseCfsMetadata.keyspace, baseCfsMetadata.indexTableName(indexMetadata), baseCfsMetadata.id)
                         .kind(TableMetadata.Kind.INDEX)
                         .partitioner(new LocalPartitioner(IntegerType.instance))
                         .addPartitionKeyColumn("pos", IntegerType.instance)
                         .addClusteringColumn("code", ByteType.instance);

        for( ColumnMetadata meta : baseCfsMetadata.primaryKeyColumns()) {
            builder.addColumn(ColumnMetadata.regularColumn( meta.ksName, meta.cfName, meta.name.toString(), meta.type));
        }

        return builder.build().updateIndexTableMetadata(baseCfsMetadata.params);
    }

    public BloomingIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        this.baseCfs = baseCfs;
        this.metadata = indexDef;

        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfs.metadata(), indexDef);
        TableMetadataRef tableRef = TableMetadataRef.forOfflineTools(indexCfsMetadata(baseCfs.metadata(), indexDef));
        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                                                             tableRef.name,
                                                             tableRef,
                                                             baseCfs.getTracker().loadsstables);
        indexedColumn = target.left;
    }


    /**
     * Extract the value to be inserted into the index from the components of the base data
     * @param partitionKey from the primary data
     * @param clustering from the primary data
     * @param path from the primary data
     * @param cellValue from the primary data
     * @return a ByteBuffer containing the value to be inserted in the index. This will be used to make the partition
     * key in the index table
     */

    private ExtendedIterator<ByteBuffer> getIndexedValues(ByteBuffer rowKey,
            Clustering<?> clustering,
            Cell<?> cell) {

        LongBuffer buff = cell == null? LongBuffer.allocate(0) : cell.buffer().asLongBuffer();
        byte[] codes = new byte[buff.remaining()*Long.BYTES];
        int pos=0;
        while (buff.hasRemaining()) {
            long word = buff.get();
            for (int i = 0; i < Long.BYTES; i++) {
                byte code = (byte) (word & 0xFF);
                word = word >> Byte.SIZE;
                codes[pos++] = code;
            }
        }

        ByteBuffer clusterKeys[] = clustering.getBufferArray();
        int capacity = Integer.BYTES+1+rowKey.capacity();
        for (ByteBuffer b  : clusterKeys) {
            capacity += b.capacity();
        }


        return WrappedIterator.create( new Iterator<ByteBuffer> () {
            byte[] code = codes;
            int pos = 0;
            @Override
            public boolean hasNext() {
                return pos<code.length;
            }

            @Override
            public ByteBuffer next() {
                if (hasNext()) {
                    ByteBuffer result = ByteBuffer.allocate(capacity);
                    result.putInt( pos );
                    result.put( code[pos++] );
                    result.put(rowKey);
                    for (ByteBuffer b : clusterKeys) {
                        result.put( b );
                    }
                    return result;
                }
                else {
                    throw new NoSuchElementException();
                }
            }
        });
    }


    private ExtendedIterator<DecoratedKey> getDecoratedKeys(ByteBuffer rowKey,
            Clustering<?> clustering,
            Cell<?> cell) {
        return getIndexedValues(rowKey, clustering, cell)

    .mapWith( (v) -> getIndexKeyFor( v ) );
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
    private <T> Clustering<?> buildIndexClustering(ByteBuffer rowKey,
            Clustering<T> clustering,
            Cell<T> cell)
{
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(rowKey);
        for (int i = 0; i < clustering.size(); i++)
            builder.add(clustering.get(i), clustering.accessor());

// Note: if indexing a static column, prefix will be Clustering.STATIC_CLUSTERING
// so the Clustering obtained from builder::build will contain a value for only
// the partition key. At query time though, this is all that's needed as the entire
// base table partition should be returned for any mathching index entry.
        return builder.build();
    }

    /**
     * Used at search time to convert a row in the index table into a simple struct containing the values required
     * to retrieve the corresponding row from the base table.
     * @param indexedValue the partition key of the indexed table (i.e. the value that was indexed)
     * @param indexEntry a row from the index table
     * @return
     */
    private IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry) {
        Clustering<?> clustering = indexEntry.clustering();

        Clustering<?> indexedEntryClustering = null;
        if (getIndexedColumn().isStatic())
            throw new IllegalStateException( "Static index columns not allowed");
        else {
            ClusteringComparator baseComparator = baseCfs.getComparator();
            CBuilder builder = CBuilder.create(baseComparator);
            for (int i = 0; i < baseComparator.size(); i++)
                builder.add(clustering, i + 1);
            indexedEntryClustering = builder.build();
        }

        return new IndexEntry(indexedValue, clustering, indexEntry.primaryKeyLivenessInfo().timestamp(),
                clustering.bufferAt(0), indexedEntryClustering);
    }

    private static <V> boolean valueIsEqual(AbstractType<?> type, Cell<V> cell, ByteBuffer value) {
        return type.compare(cell.value(), cell.accessor(), value, ByteBufferAccessor.instance) == 0;
    }

    /**
     * Check whether a value retrieved from an index is still valid by comparing it to current row from the base table.
     * Used at read time to identify out of date index entries so that they can be excluded from search results and
     * repaired
     * @param row the current row from the primary data table
     * @param indexValue the value we retrieved from the index
     * @param nowInSec
     * @return true if the index is out of date and the entry should be dropped
     */
    private boolean isStale(Row data, ByteBuffer indexValue, int nowInSec) {
        Cell<?> cell = data.getCell(indexedColumn);
        return cell == null || !cell.isLive(nowInSec) || !valueIsEqual(indexedColumn.type, cell, indexValue);
    }



    /**
     * Returns true if an index of this type can support search predicates of the form [column] OPERATOR [value]
     * @param indexedColumn
     * @param operator
     * @return
     */
    protected boolean supportsOperator(ColumnMetadata indexedColumn, Operator operator)
    {
        return operator == Operator.EQ;
    }


    public ColumnMetadata getIndexedColumn()
    {
        return indexedColumn;
    }

    public ClusteringComparator getIndexComparator()
    {
        return indexCfs.metadata().comparator;
    }

    public ColumnFamilyStore getIndexCfs()
    {
        return indexCfs;
    }

    @Override
    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    @Override
    public Callable<?> getInitializationTask()
    {
        // if we're just linking in the index on an already-built index post-restart or if the base
        // table is empty we've nothing to do. Otherwise, submit for building via SecondaryIndexBuilder
        return isBuilt() || baseCfs.isEmpty() ? null : getBuildIndexTask();
    }

    @Override
    public IndexMetadata getIndexMetadata()
    {
        return metadata;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return indexCfs == null ? Optional.empty() : Optional.of(indexCfs);
    }

    @Override
    public Callable<Void> getBlockingFlushTask()
    {
        return () -> {
            indexCfs.forceBlockingFlush();
            return null;
        };
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return () -> {
            invalidate();
            return null;
        };
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexDef)
    {
        return () -> {
            indexCfs.reload();
            return null;
        };
    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException
    {
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent())
        {
            ByteBuffer indexValue = target.get().getIndexValue();
            checkFalse(indexValue.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                       "Index expression values may not be larger than 64K");
        }
    }


    @Override
    public Callable<?> getTruncateTask(final long truncatedAt)
    {
        return () -> {
            indexCfs.discardSSTables(truncatedAt);
            return null;
        };
    }

    @Override
    public boolean shouldBuildBlocking()
    {
        // built-in indexes are always included in builds initiated from SecondaryIndexManager
        return true;
    }

    @Override
    public boolean dependsOn(ColumnMetadata column)
    {
        return indexedColumn.name.equals(column.name);
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return indexedColumn.name.equals(column.name)
               && supportsOperator(indexedColumn, operator);
    }

    private boolean supportsExpression(RowFilter.Expression expression)
    {
        return supportsExpression(expression.column(), expression.operator());
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    @Override
    public long getEstimatedResultRows()
    {
        return indexCfs.getMeanRowCount();
    }

    /**
     * No post processing of query results, just return them unchanged
     */
    @Override
    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command)
    {
        return (partitionIterator, readCommand) -> partitionIterator;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return getTargetExpression(filter.getExpressions()).map(filter::without)
                                                           .orElse(filter);
    }

    private Optional<RowFilter.Expression> getTargetExpression(List<RowFilter.Expression> expressions)
    {
        return expressions.stream().filter(this::supportsExpression).findFirst();
    }

    @Override
    public Index.Searcher searcherFor(ReadCommand command)
    {
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent())
        {
            return new BloomSearcher( command, target.get(), this );
        }

        return null;

    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        switch (indexedColumn.kind)
        {
            case PARTITION_KEY:
                validatePartitionKey(update.partitionKey());
                break;
            case CLUSTERING:
                validateClusterings(update);
                break;
            case REGULAR:
                if (update.columns().regulars.contains(indexedColumn))
                    validateRows(update);
                break;
            case STATIC:
                if (update.columns().statics.contains(indexedColumn))
                    validateRows(Collections.singleton(update.staticRow()));
                break;
        }
    }

    @Override
    public Indexer indexerFor(final DecoratedKey key,
                              final RegularAndStaticColumns columns,
                              final int nowInSec,
                              final WriteContext ctx,
                              final IndexTransaction.Type transactionType)
    {
        /**
         * Indexes on regular and static columns (the non primary-key ones) only care about updates with live
         * data for the column they index. In particular, they don't care about having just row or range deletions
         * as they don't know how to update the index table unless they know exactly the value that is deleted.
         *
         * Note that in practice this means that those indexes are only purged of stale entries on compaction,
         * when we resolve both the deletion and the prior data it deletes. Of course, such stale entries are also
         * filtered on read.
         */
        if (!isPrimaryKeyIndex() && !columns.contains(indexedColumn))
            return null;

        return new Indexer()
        {
            @Override
            public void begin()
            {
            }

            @Override
            public void partitionDelete(DeletionTime deletionTime)
            {
            }

            @Override
            public void rangeTombstone(RangeTombstone tombstone)
            {
            }

            @Override
            public void insertRow(Row row)
            {
                if (row.isStatic() && !indexedColumn.isStatic() && !indexedColumn.isPartitionKey())
                    return;

                if (isPrimaryKeyIndex())
                {
                    indexPrimaryKey(row.clustering(),
                                    getPrimaryKeyIndexLiveness(row),
                                    row.deletion());
                }
                else
                {
                    if (indexedColumn.isComplex())
                        indexCells(row.clustering(), row.getComplexColumnData(indexedColumn));
                    else
                        indexCell(row.clustering(), row.getCell(indexedColumn));
                }
            }

            @Override
            public void removeRow(Row row)
            {
                if (isPrimaryKeyIndex())
                    return;

                if (indexedColumn.isComplex())
                    removeCells(row.clustering(), row.getComplexColumnData(indexedColumn));
                else
                    removeCell(row.clustering(), row.getCell(indexedColumn));
            }

            @Override
            public void updateRow(Row oldRow, Row newRow)
            {
                assert oldRow.isStatic() == newRow.isStatic();
                if (newRow.isStatic() != indexedColumn.isStatic())
                    return;

                if (isPrimaryKeyIndex())
                    indexPrimaryKey(newRow.clustering(),
                                    getPrimaryKeyIndexLiveness(newRow),
                                    newRow.deletion());

                if (indexedColumn.isComplex())
                {
                    indexCells(newRow.clustering(), newRow.getComplexColumnData(indexedColumn));
                    removeCells(oldRow.clustering(), oldRow.getComplexColumnData(indexedColumn));
                }
                else
                {
                    indexCell(newRow.clustering(), newRow.getCell(indexedColumn));
                    removeCell(oldRow.clustering(), oldRow.getCell(indexedColumn));
                }
            }

            @Override
            public void finish()
            {
            }

            private void indexCells(Clustering<?> clustering, Iterable<Cell<?>> cells)
            {
                if (cells == null)
                    return;

                for (Cell<?> cell : cells)
                    indexCell(clustering, cell);
            }

            private void indexCell(Clustering<?> clustering, Cell<?> cell)
            {
                if (cell == null || !cell.isLive(nowInSec))
                    return;

                insert(key.getKey(),
                       clustering,
                       cell,
                       LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime()),
                       ctx);
            }

            private void removeCells(Clustering<?> clustering, Iterable<Cell<?>> cells)
            {
                if (cells == null)
                    return;

                for (Cell<?> cell : cells)
                    removeCell(clustering, cell);
            }

            private void removeCell(Clustering<?> clustering, Cell<?> cell)
            {
                if (cell == null || !cell.isLive(nowInSec))
                    return;

                delete(key.getKey(), clustering, cell, ctx, nowInSec);
            }

            private void indexPrimaryKey(final Clustering<?> clustering,
                                         final LivenessInfo liveness,
                                         final Row.Deletion deletion)
            {
                if (liveness.timestamp() != LivenessInfo.NO_TIMESTAMP)
                    insert(key.getKey(), clustering, null, liveness, ctx);

                if (!deletion.isLive())
                    delete(key.getKey(), clustering, deletion.time(), ctx);
            }

            private LivenessInfo getPrimaryKeyIndexLiveness(Row row)
            {
                long timestamp = row.primaryKeyLivenessInfo().timestamp();
                int ttl = row.primaryKeyLivenessInfo().ttl();
                for (Cell<?> cell : row.cells())
                {
                    long cellTimestamp = cell.timestamp();
                    if (cell.isLive(nowInSec))
                    {
                        if (cellTimestamp > timestamp)
                            timestamp = cellTimestamp;
                    }
                }
                return LivenessInfo.create(timestamp, ttl, nowInSec);
            }
        };
    }

    /**
     * Specific to internal indexes, this is called by a
     * searcher when it encounters a stale entry in the index
     * @param indexKey the partition key in the index table
     * @param indexClustering the clustering in the index table
     * @param deletion deletion timestamp etc
     * @param ctx the write context under which to perform the deletion
     */
    public void deleteStaleEntry(DecoratedKey indexKey,
                                 Clustering<?> indexClustering,
                                 DeletionTime deletion,
                                 WriteContext ctx)
    {
        doDelete(indexKey, indexClustering, deletion, ctx);
        logger.trace("Removed index entry for stale value {}", indexKey);
    }

    /**
     * Called when adding a new entry to the index
     */
    private void insert(ByteBuffer rowKey,
                        Clustering<?> clustering,
                        Cell<?> cell,
                        LivenessInfo info,
                        WriteContext ctx)
    {
        Row row = BTreeRow.noCellLiveRow(buildIndexClustering(rowKey, clustering, cell), info);

        getDecoratedKeys(rowKey, clustering, cell)
        .forEach( (valueKey) -> {

        PartitionUpdate upd = partitionUpdate(valueKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
        logger.trace("Inserted entry into index for value {}", valueKey);
        } );
    }

    /**
     * Called when deleting entries on non-primary key columns
     */
    private void delete(ByteBuffer rowKey,
                        Clustering<?> clustering,
                        Cell<?> cell,
                        WriteContext ctx,
                        int nowInSec)
    {
        getDecoratedKeys(rowKey, clustering, cell)
        .forEach( (valueKey) -> {

        doDelete(valueKey,
                 buildIndexClustering(rowKey, clustering, cell),
                 new DeletionTime(cell.timestamp(), nowInSec),
                 ctx);
        });
    }

//    /**
//     * Called when deleting entries from indexes on primary key columns
//     */
//    private void delete(ByteBuffer rowKey,
//                        Clustering<?> clustering,
//                        DeletionTime deletion,
//                        WriteContext ctx)
//    {
//        getDecoratedKeys(rowKey, clustering, null)
//        .forEach( (valueKey) -> {
//        doDelete(valueKey,
//                 buildIndexClustering(rowKey, clustering, null),
//                 deletion,
//                 ctx);
//        });
//    }

    private void doDelete(DecoratedKey indexKey,
                          Clustering<?> indexClustering,
                          DeletionTime deletion,
                          WriteContext ctx)
    {
        Row row = BTreeRow.emptyDeletedRow(indexClustering, Row.Deletion.regular(deletion));
        PartitionUpdate upd = partitionUpdate(indexKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, UpdateTransaction.NO_OP);
        logger.trace("Removed index entry for value {}", indexKey);
    }

    private void validatePartitionKey(DecoratedKey partitionKey) throws InvalidRequestException
    {
        assert indexedColumn.isPartitionKey();
        validateIndexedValue(getIndexedValue(partitionKey.getKey(), null, null));
    }

    private void validateClusterings(PartitionUpdate update) throws InvalidRequestException
    {
        assert indexedColumn.isClusteringColumn();
        for (Row row : update)
            validateIndexedValue(getIndexedValue(null, row.clustering(), null));
    }

    private void validateRows(Iterable<Row> rows)
    {
        assert !indexedColumn.isPrimaryKeyColumn();
        for (Row row : rows)
        {
            if (indexedColumn.isComplex())
            {
                ComplexColumnData data = row.getComplexColumnData(indexedColumn);
                if (data != null)
                {
                    for (Cell<?> cell : data)
                    {
                        validateIndexedValue(getIndexedValue(null, null, cell.path(), cell.buffer()));
                    }
                }
            }
            else
            {
                validateIndexedValue(getIndexedValue(null, null, row.getCell(indexedColumn)));
            }
        }
    }

    private void validateIndexedValue(ByteBuffer value)
    {
        if (value != null && value.remaining() >= FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format(
                                                           "Cannot index value of size %d for index %s on %s(%s) (maximum allowed size=%d)",
                                                           value.remaining(),
                                                           metadata.name,
                                                           baseCfs.metadata,
                                                           indexedColumn.name.toString(),
                                                           FBUtilities.MAX_UNSIGNED_SHORT));
    }





    private DecoratedKey getIndexKeyFor(ByteBuffer value)
    {
        return indexCfs.decorateKey(value);
    }

    private PartitionUpdate partitionUpdate(DecoratedKey valueKey, Row row)
    {
        return PartitionUpdate.singleRowUpdate(indexCfs.metadata(), valueKey, row);
    }

    private void invalidate()
    {
        // interrupt in-progress compactions
        Collection<ColumnFamilyStore> cfss = Collections.singleton(indexCfs);
        CompactionManager.instance.interruptCompactionForCFs(cfss, (sstable) -> true, true);
        CompactionManager.instance.waitForCessation(cfss, (sstable) -> true);
        Keyspace.writeOrder.awaitNewBarrier();
        indexCfs.forceBlockingFlush();
        indexCfs.readOrdering.awaitNewBarrier();
        indexCfs.invalidate();
    }

    private boolean isBuilt()
    {
        return SystemKeyspace.isIndexBuilt(baseCfs.keyspace.getName(), metadata.name);
    }

    private boolean isPrimaryKeyIndex()
    {
        return indexedColumn.isPrimaryKeyColumn();
    }

    private Callable<?> getBuildIndexTask()
    {
        return () -> {
            buildBlocking();
            return null;
        };
    }

    @SuppressWarnings("resource")
    private void buildBlocking()
    {
        baseCfs.forceBlockingFlush();

        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
             Refs<SSTableReader> sstables = viewFragment.refs)
        {
            if (sstables.isEmpty())
            {
                logger.info("No SSTable data for {}.{} to build index {} from, marking empty index as built",
                            baseCfs.metadata.keyspace,
                            baseCfs.metadata.name,
                            metadata.name);
                return;
            }

            logger.info("Submitting index build of {} for data in {}",
                        metadata.name,
                        getSSTableNames(sstables));

            SecondaryIndexBuilder builder = new CollatedViewIndexBuilder(baseCfs,
                                                                         Collections.singleton(this),
                                                                         new ReducingKeyIterator(sstables),
                                                                         ImmutableSet.copyOf(sstables));
            Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
            FBUtilities.waitOnFuture(future);
            indexCfs.forceBlockingFlush();
        }
        logger.info("Index build of {} complete", metadata.name);
    }

    private static String getSSTableNames(Collection<SSTableReader> sstables)
    {
        return StreamSupport.stream(sstables.spliterator(), false)
                            .map(SSTableReader::toString)
                            .collect(Collectors.joining(", "));
    }


    /**
     * Factory method for new CassandraIndex instances
     * @param baseCfs
     * @param indexMetadata
     * @return
     */
    public static CassandraIndex newIndex(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
    {
        return getFunctions(indexMetadata, TargetParser.parse(baseCfs.metadata(), indexMetadata)).newIndexInstance(baseCfs, indexMetadata);
    }

    static CassandraIndexFunctions getFunctions(IndexMetadata indexDef,
                                                Pair<ColumnMetadata, IndexTarget.Type> target)
    {
        if (indexDef.isKeys())
            return CassandraIndexFunctions.KEYS_INDEX_FUNCTIONS;

        ColumnMetadata indexedColumn = target.left;
        if (indexedColumn.type.isCollection() && indexedColumn.type.isMultiCell())
        {
            switch (((CollectionType)indexedColumn.type).kind)
            {
                case LIST:
                    return CassandraIndexFunctions.COLLECTION_VALUE_INDEX_FUNCTIONS;
                case SET:
                    return CassandraIndexFunctions.COLLECTION_KEY_INDEX_FUNCTIONS;
                case MAP:
                    switch (target.right)
                    {
                        case KEYS:
                            return CassandraIndexFunctions.COLLECTION_KEY_INDEX_FUNCTIONS;
                        case KEYS_AND_VALUES:
                            return CassandraIndexFunctions.COLLECTION_ENTRY_INDEX_FUNCTIONS;
                        case VALUES:
                            return CassandraIndexFunctions.COLLECTION_VALUE_INDEX_FUNCTIONS;
                    }
                    throw new AssertionError();
            }
        }

        switch (indexedColumn.kind)
        {
            case CLUSTERING:
                return CassandraIndexFunctions.CLUSTERING_COLUMN_INDEX_FUNCTIONS;
            case REGULAR:
            case STATIC:
                return CassandraIndexFunctions.REGULAR_COLUMN_INDEX_FUNCTIONS;
            case PARTITION_KEY:
                return CassandraIndexFunctions.PARTITION_KEY_INDEX_FUNCTIONS;
            //case COMPACT_VALUE:
            //    return new CompositesIndexOnCompactValue();
        }
        throw new AssertionError();
    }
}

}
