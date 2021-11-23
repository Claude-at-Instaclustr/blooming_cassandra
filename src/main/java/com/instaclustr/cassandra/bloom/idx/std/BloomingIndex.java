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
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.internal.CollatedViewIndexBuilder;
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

        TableMetadata.Builder builder =
                TableMetadata.builder(baseCfsMetadata.keyspace, baseCfsMetadata.indexTableName(indexMetadata), baseCfsMetadata.id)
                .kind(TableMetadata.Kind.INDEX)
                .partitioner(new LocalPartitioner(LongType.instance))
                .addPartitionKeyColumn("pos", Int32Type.instance)
                .addPartitionKeyColumn("code", Int32Type.instance)
                .addClusteringColumn("dataKey", BytesType.instance);

        for( ColumnMetadata meta : baseCfsMetadata.primaryKeyColumns()) {
            builder.addColumn(ColumnMetadata.regularColumn( meta.ksName, meta.cfName, meta.name.toString(), meta.type));
        }

        return builder.build().updateIndexTableMetadata(baseCfsMetadata.params);
    }

    public BloomingIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        this.baseCfs = baseCfs;
        this.metadata = indexDef;

        serde = new BloomingIndexSerde( baseCfs, indexDef );

        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfs.metadata(), indexDef);
        indexedColumn = target.left;
        if (indexedColumn.isClusteringColumn() || indexedColumn.isComplex() ||
                indexedColumn.isCounterColumn() || indexedColumn.isPartitionKey() ||
                indexedColumn.isPrimaryKeyColumn() || indexedColumn.isStatic()){
            throw new IllegalArgumentException( "index column may not be culstering column, complex column, "
                    + "counter column, partition key, primary key column, or static column");
        }
    }

    public ColumnMetadata getIndexedColumn()
    {
        return indexedColumn;
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
                && operator == Operator.EQ;
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
            return new BloomSearcher( this, serde, command, target.get());
        }

        return null;

    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        assert !indexedColumn.isPrimaryKeyColumn();

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
}


