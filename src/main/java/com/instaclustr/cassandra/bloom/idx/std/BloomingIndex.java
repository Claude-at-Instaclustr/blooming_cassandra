package com.instaclustr.cassandra.bloom.idx.std;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
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
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexFunctions;
import org.apache.cassandra.index.internal.CollatedViewIndexBuilder;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class BloomingIndex implements Index {

    private static final Logger logger = LoggerFactory.getLogger(BloomingIndex.class);

    public final ColumnFamilyStore baseCfs;
    protected IndexMetadata metadata;
    private BloomingIndexSerde serde;
    private ColumnMetadata indexedColumn;


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
        serde = new BloomingIndexSerde( ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                tableRef.name,
                tableRef,
                baseCfs.getTracker().loadsstables) );
        indexedColumn = target.left;
        if (indexedColumn.isClusteringColumn() || indexedColumn.isComplex() ||
                indexedColumn.isCounterColumn() || indexedColumn.isPartitionKey() ||
                indexedColumn.isPrimaryKeyColumn() || indexedColumn.isStatic()){
            throw new IllegalArgumentException( "index column may not be culstering column, complex column, "
                    + "counter column, partition key, primary key column, or static column");
        }
    }





    private ExtendedIterator<DecoratedKey> getDecoratedKeys(ByteBuffer rowKey,
            Clustering<?> clustering,
            Cell<?> cell) {
        return BFUtils.getIndexedValues(rowKey, clustering, cell).mapWith( v -> {return serde.getIndexKeyFor(v);} );
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
    //
    //    public ClusteringComparator getIndexComparator()
    //    {
    //        return indexCfs.metadata().comparator;
    //    }

    //    public ColumnFamilyStore getIndexCfs()
    //    {
    //        return indexCfs;
    //    }

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
        return serde.getBackingTable();
    }

    @Override
    public Callable<Void> getBlockingFlushTask()
    {
        return () -> {
            serde.forceBlockingFlush();
            return null;
        };
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return () -> {
            serde.invalidate();
            return null;
        };
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexDef)
    {
        return () -> {
            serde.reload();
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
            serde.truncate(truncatedAt);
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
        return serde.getEstimatedResultRows();
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
            return new BloomSearcher( serde, command, target.get());
        }

        return null;

    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        assert !indexedColumn.isPrimaryKeyColumn();

        //update.forEach(null);
        WrappedIterator.create(update.iterator()).mapWith( r -> r.getCell(indexedColumn).buffer() )
        .filterDrop( b -> b == null ).forEach( v -> {if (v.remaining() >= FBUtilities.MAX_UNSIGNED_SHORT) {
            throw new InvalidRequestException(String.format(
                    "Cannot index value of size %d for index %s on %s(%s) (maximum allowed size=%d)",
                    v.remaining(),
                    metadata.name,
                    baseCfs.metadata,
                    indexedColumn.name.toString(),
                    FBUtilities.MAX_UNSIGNED_SHORT));}});

    }

    @Override
    public Indexer indexerFor(final DecoratedKey key,
            final RegularAndStaticColumns columns,
            final int nowInSec,
            final WriteContext ctx,
            final IndexTransaction.Type transactionType)
    {
        return columns.contains(indexedColumn) ? new BloomingIndexer( key, serde, indexedColumn, nowInSec, ctx, transactionType ) : null;
    }

    private boolean isBuilt()
    {
        return SystemKeyspace.isIndexBuilt(baseCfs.keyspace.getName(), metadata.name);
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
            serde.forceBlockingFlush();
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


