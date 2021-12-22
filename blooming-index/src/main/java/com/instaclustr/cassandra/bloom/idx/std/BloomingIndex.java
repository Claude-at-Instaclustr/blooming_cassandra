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

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
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
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
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
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.instaclustr.iterator.util.ExtendedIterator;
import com.instaclustr.iterator.util.WrappedIterator;

/**
 * An index that implements a multidimensional Bloom filter index.
 * <p>
 * The index is applied to Blob columns that contain Bloom filters.  Bloom filters are represented
 * as an array of bytes where enabled bits in the bytes represent enabled bits in the complete filter.
 * The ordering of the bytes is not specified but must be the same for all filters written to the index.
 * </p>
 *
 * <p>Bloom filters may contain multiple items and do not have to be composed from data stored in the row.</p>
 *
 * <h2>False Positives</h2>
 *
 * <p>By their nature Bloom filters can yield false positives.  Therefore, values returned by this index
 * should be verified to ensure they actually meet the expected criteria</p>
 *
 * <h2>Example operational flow</h2>
 *
 * <h3>Assumptions</h3>
 * <ol>
 * <li>There is an object that has multiple names and a location.</li>
 * <li>There is a Cassandra table that stores that object, has a blob column to store the Bloom filter,
 * and has a {@code BloomingIndex} on that column.</li>
 * </ol>
 *
 * <h3>Workflow</h3>
 * <ol>
 * <li> A Bloom filter is constructed for each row using all the alternate names and the location
 * for the object</li>
 * <li> The rows are added to the table.</li>
 * <li> We want to find all things names "Las Vegas" that are the "US" so we construct Bloom filter
 * from the values "Las Vegas" and "US"</li>
 * <li> We convert the Bloom filter byte array into the Cassandra blob format.</li>
 * <li> We perform a search on the table with the query
 * {@code SELECT * FROM table WHERE bloomFilterColumn = filterBlob}</li>
 * <li> For each returned row we verify that one of the alternate names is "Las Vegas" and that the
 * location is "US".</li>
 * </ol>
 *
 * <h2>Options</h2>
 *
 * <p>The following options may be specified.  If one is specified all should be specified.  If all
 * are specified they are used in the calculation to determine how many rows this index is applied to.
 * If they are not specified the index is assumed to apply to all rows in the base table.</p>
 *
 * <dl>
 * <dt>numberOfBits</dt>
 * <dd>
 * The maximum number of bits in a filter.  Often called {@code m} when describing Bloom filters.
 * </dd>
 * <dt>numberOfFunctions</dt>
 * <dd>
 * The number of hash functions applied to an item as it is added to the Bloom filter.  Often
 * called {@code k} when describing Bloom filters.
 * </dd>
 * <dt>numberOfItems</dt>
 * <dd>
 * The average number of items in each Bloom filter added to the index.  Often
 * called {@code n} when describing Bloom filters.
 * </dd>
 * <dt>usePrimaryFilter</dt>
 * <dd>
 * If {@code true} then when retrieving baseTable keys use a Bloom filter to filter out non-matching keys.  This trades smaller
 * memory requirement for slower speed.
 * </dd>
 * </dl>
 *
 */
public class BloomingIndex implements Index {

    private static final Logger logger = LoggerFactory.getLogger(BloomingIndex.class);

    /**
     * The a base table where the data are stored.
     */
    private final ColumnFamilyStore baseCfs;
    /**
     * The name of the index column in the base table
     */
    private ColumnMetadata indexedColumn;
    /**
     * The metadata for the index.
     */
    protected IndexMetadata metadata;
    /**
     * The Serde to use to read/write the index table.
     */
    private BloomingIndexSerde serde;

    /**
     * The estimated number of entries in the index table per row of the base table.
     * may be 0.0;
     */
    private final double indexEntriesPerRow;

    private final boolean usePrimaryFilter;

    /**
     * Constructor
     * @param baseCfs  The the base table.
     * @param indexDef the the index definition.
     */
    public BloomingIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
        logger.debug("Constructor");
        this.baseCfs = baseCfs;
        this.metadata = indexDef;

        serde = new BloomingIndexSerde(baseCfs, indexDef);

        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfs.metadata(), indexDef);
        indexedColumn = target.left;
        if (indexedColumn.isClusteringColumn() || indexedColumn.isComplex() || indexedColumn.isCounterColumn()
                || indexedColumn.isPartitionKey() || indexedColumn.isPrimaryKeyColumn() || indexedColumn.isStatic()) {
            throw new IllegalArgumentException("Bloom filter column may not be culstering column, complex column, "
                    + "counter column, partition key, primary key column, or static column");
        }

        // parse ints but store as doubles.
        // The maximum number of bits in the Bloom filter (May be 0.0)

        final double numberOfBits = parseInt(indexDef.options, "numberOfBits");

        // The maximum number of hash functions used for each item in the Bloom filter
        // (May be 0.0)
        final double numberOfFunctions = parseInt(indexDef.options, "numberOfFunctions");

        // The maximum number of items the Bloom filter (May be 0.0)
        final double numberOfItems = parseInt(indexDef.options, "numberOfItems");

        boolean calculateRatio = true;
        // it at least one was specified
        if (numberOfBits >= 0.0 || numberOfFunctions >= 0.0 || numberOfItems >= 0.0) {
            // then if any is not specified or is zero.
            if (numberOfBits <= 0.0) {
                logger.warn("index created with numberOfBits ({}) <= zero", numberOfBits);
                calculateRatio = false;
            }
            if (numberOfItems <= 0.0) {
                logger.warn("index created with numberOfItems ({}) <= zero", numberOfItems);
                calculateRatio = false;
            }
            if (numberOfFunctions <= 0.0) {
                logger.warn("index created with numberOfFunctions ({}) <= zero", numberOfFunctions);
                calculateRatio = false;
            }
        }

        indexEntriesPerRow = calculateRatio ? calculateIndexPerRow(numberOfBits, numberOfItems, numberOfFunctions)
                : 0.0;
        usePrimaryFilter = toBoolean(indexDef.options.get("usePrimaryFilter"), false);
    }

    /**
     * Parses integer values from a map of options.
     * @param options the Map of options name-value pairs
     * @param option the option to extract the value for.
     * @return the option value or 0 (zero) if the value was not set in the map.
     * @throws IllegalArgumentexception if the option value could not be parsed as an integer.
     */
    public static int parseInt(Map<String, String> options, String option) {
        String value = options.get(option);
        try {
            return value == null ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Value for option '%s' is not an integer", option), e);
        }
    }

    private static double calculateIndexPerRow(double m, double n, double k) {

        // kn = number of bits requested from hasher
        double kn = k * n;

        // @formatter:off
        //
        // the probability of a collision when selecting from a population of i
        // in a range of [1; m] is:
        //
        //                        i
        //                / m - 1 \
        // q(i;m) =  1 - |  -----  |
        //                \   m   /
        //
        //
        // The probability that the ith integer randomly chosen from
        // [1, m] will repeat a previous choice equals q(i − 1; m) so the
        // total expected collisions in kn selections is
        //
        //      kn
        //     =====                                    kn
        //      \                               / m - 1 \
        //       >    q(i - 1; m ) = kn - m + m|  ----- |
        //      /                               \   m   /
        //      =====
        //      i = 1
        //
        // @formatter:on
        double collisions = kn - m + m * Math.pow((m - 1) / m, kn);
        // expected number of bits per entry
        double bits = kn - collisions;
        /*
         * the number of index entries per row is the lesser of the number of bits or
         * the number of bytes in the bloom filter. The reasoning here is that the bits
         * are evenly distributed across the bloom filter, so the probability of a bit
         * being in the same byte as another bit reaches 1 when there are more bits than
         * bytes. Since we only record bytes in the index we need the minimum of the two
         * values.
         */
        return Math.min(bits, m / Byte.SIZE);

    }

    @Override
    public void register(IndexRegistry registry) {
        logger.debug("register");
        registry.registerIndex(this);
    }

    @Override
    public Callable<?> getInitializationTask() {
        logger.debug("getInitializationTask");

        // if we're just linking in the index on an already-built index post-restart or
        // if the base
        // table is empty we've nothing to do. Otherwise, submit for building via
        // SecondaryIndexBuilder
        return isBuilt() || baseCfs.isEmpty() ? null : getBuildIndexTask();
    }

    @Override
    public IndexMetadata getIndexMetadata() {
        logger.debug("getIndexMetadata");
        return metadata;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable() {
        logger.debug("getBackingTable");
        return Optional.of(serde.getBackingTable());
    }

    @Override
    public Callable<Void> getBlockingFlushTask() {
        logger.debug("getBlockingFlushTask");
        return () -> {
            serde.forceBlockingFlush();
            return null;
        };
    }

    @Override
    public Callable<?> getInvalidateTask() {
        logger.debug("getInvalidateTask");
        return () -> {
            serde.invalidate();
            return null;
        };
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexDef) {
        logger.debug("getMetadataReloadTask");
        return () -> {
            serde.reload();
            return null;
        };
    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException {
        logger.debug("validate");
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent()) {
            ByteBuffer indexValue = target.get().getIndexValue();
            checkFalse(indexValue.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                    "Index expression values may not be larger than 64K");
        }
    }

    @Override
    public Callable<?> getTruncateTask(final long truncatedAt) {
        logger.debug("getTruncateTask");
        return () -> {
            serde.truncate(truncatedAt);
            return null;
        };
    }

    @Override
    public boolean shouldBuildBlocking() {
        logger.debug("shouldBuildBlocking");
        // built-in indexes are always included in builds initiated from
        // SecondaryIndexManager
        return true;
    }

    @Override
    public boolean dependsOn(ColumnMetadata column) {
        logger.debug("dependsOn");
        return indexedColumn.name.equals(column.name);
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator) {
        logger.debug("supportsExpression");
        return indexedColumn.name.equals(column.name) && operator == Operator.EQ;
    }

    /**
     * Determine if this index can provide a searcher for a RowFilter Expression.
     * @param expression the Row.Filter expression.
     * @return {@code true} if this filter supports the expression, {@code false} otherwise.
     * @see #supportsExpression(ColumnMetadata, Operator)
     */
    private boolean supportsExpression(RowFilter.Expression expression) {
        return supportsExpression(expression.column(), expression.operator());
    }

    @Override
    public AbstractType<?> customExpressionValueType() {
        logger.debug("customExpressionValueType");
        return null;
    }

    @Override
    public boolean supportsReplicaFilteringProtection(RowFilter arg0) {
        return false;
    }

    @Override
    public long getEstimatedResultRows() {
        logger.debug("getEstimatedResultRows");
        logger.debug("baseCfs estimateKeys {}", baseCfs.estimateKeys());
        logger.debug("baseCfs getMeanPartitionSize {}", baseCfs.getMeanPartitionSize());
        logger.debug("baseCfs getMeanRowCount {}", baseCfs.getMeanRowCount());

        // If any of the parameters are not set and there is data in the index
        // then serde.getEstimatedResultRows() will return -1.
        // in this case we asume the index is used on all the rows in the base table.
        long result = serde.getEstimatedResultRows(indexEntriesPerRow);
        result = result == -1 ? baseCfs.estimateKeys() : result;
        logger.debug("getEstimatedResultRows returning {}", result);
        return result;
    }

    @Override
    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command) {
        logger.debug("postProcessorFor");

        /*
         * this looks a bit messy but we are building a partition iterators that only
         * return unique rowKey, and row clustering combinations. This is the same
         * combination that is used by BloomingIndexSerde to create clustering for the
         * internal tables.
         */
        return new BiFunction<PartitionIterator, ReadCommand, PartitionIterator>() {
            Set<Clustering<?>> seen = new HashSet<Clustering<?>>();

            @Override
            public PartitionIterator apply(PartitionIterator t, ReadCommand u) {
                return new PartitionIterator() {
                    private PartitionIterator wrappedPI = t;

                    @Override
                    public void close() {
                        wrappedPI.close();
                    }

                    @Override
                    public boolean hasNext() {
                        return wrappedPI.hasNext();
                    }

                    @Override
                    public RowIterator next() {
                        return new RowIterator() {
                            private RowIterator wrappedRI = wrappedPI.next();
                            private ExtendedIterator<Row> ext = WrappedIterator.create(wrappedRI).filterKeep(row -> {
                                Clustering<?> cluster = serde.buildIndexClustering(wrappedRI.partitionKey(),
                                        row.clustering());
                                return seen.add(cluster);
                            });

                            @Override
                            public RegularAndStaticColumns columns() {
                                return wrappedRI.columns();
                            }

                            @Override
                            public void forEachRemaining(Consumer<? super Row> arg0) {
                                wrappedRI.forEachRemaining(arg0);
                            }

                            @Override
                            public TableMetadata metadata() {
                                return wrappedRI.metadata();
                            }

                            @Override
                            public boolean isReverseOrder() {
                                return wrappedRI.isReverseOrder();
                            }

                            @Override
                            public boolean isEmpty() {
                                return wrappedRI.isEmpty();
                            }

                            @Override
                            public DecoratedKey partitionKey() {
                                return wrappedRI.partitionKey();
                            }

                            @Override
                            public void remove() {
                                wrappedRI.remove();
                            }

                            @Override
                            public Row staticRow() {
                                return wrappedRI.staticRow();
                            }

                            @Override
                            public void close() {
                                wrappedRI.close();
                            }

                            @Override
                            public boolean hasNext() {
                                return ext.hasNext();
                            }

                            @Override
                            public Row next() {
                                return ext.next();
                            }
                        };
                    }
                };
            }
        };
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter) {
        logger.debug("getPostIndexQueryFilter");
        return getTargetExpression(filter.getExpressions()).map(filter::without).orElse(filter);
    }

    /**
     * Finds the first RowFilter.Expression that this index supports
     * @param expressions a list of RowFilter Expressions to check.
     * @return the first match or an empty optional.
     */
    private Optional<RowFilter.Expression> getTargetExpression(List<RowFilter.Expression> expressions) {
        return expressions.stream().filter(this::supportsExpression).findFirst();
    }

    @Override
    public Index.Searcher searcherFor(ReadCommand command) {
        logger.debug("searcherFor");
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent()) {
            return new BloomingSearcher(usePrimaryFilter, indexedColumn, baseCfs, serde, command, target.get());
        }

        return null;

    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException {
        logger.debug("validate");
        assert !indexedColumn.isPrimaryKeyColumn();

        try {
            WrappedIterator.create(update.iterator()).mapWith(r -> r.getCell(indexedColumn)).filterDrop(b -> b == null)
            .mapWith(Cell::buffer).forEach(v -> {
                if (v.remaining() >= FBUtilities.MAX_UNSIGNED_SHORT) {
                    throw new InvalidRequestException(String.format(
                            "Cannot index value of size %d for index %s on %s(%s) (maximum allowed size=%d)",
                            v.remaining(), metadata.name, baseCfs.metadata, indexedColumn.name.toString(),
                            FBUtilities.MAX_UNSIGNED_SHORT));
                }
            });
        } catch (Exception e) {
            throw new InvalidRequestException(e.getMessage(), e);
        }
    }

    @Override
    public Indexer indexerFor(final DecoratedKey key, final RegularAndStaticColumns columns, final int nowInSec,
            final WriteContext ctx, final IndexTransaction.Type transactionType) {
        logger.debug("indexerFor");
        return columns.contains(indexedColumn) ? new BloomingIndexer(key, baseCfs, serde, indexedColumn, nowInSec, ctx)
                : null;
    }

    /**
     * Determines if this index is built by asking the base table.
     * @return true if this index was built.
     */
    private boolean isBuilt() {
        return SystemKeyspace.isIndexBuilt(baseCfs.keyspace.getName(), metadata.name);
    }

    /**
     * Constructs the callable task to build the index.
     * @return The callable task to build the index.
     */
    private Callable<?> getBuildIndexTask() {
        return () -> {
            buildBlocking();
            return null;
        };
    }

    /**
     * Build the index using a blocking strategy
     */
    private void buildBlocking() {
        baseCfs.forceBlockingFlush();

        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs
                .selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
                Refs<SSTableReader> sstables = viewFragment.refs) {
            if (sstables.isEmpty()) {
                logger.info("No SSTable data for {}.{} to build index {} from, marking empty index as built",
                        baseCfs.metadata.keyspace, baseCfs.metadata.name, metadata.name);
                return;
            }

            logger.info("Submitting index build of {} for data in {}", metadata.name, getSSTableNames(sstables));

            SecondaryIndexBuilder builder = new CollatedViewIndexBuilder(baseCfs, Collections.singleton(this),
                    new ReducingKeyIterator(sstables), ImmutableSet.copyOf(sstables));
            Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
            FBUtilities.waitOnFuture(future);
            serde.forceBlockingFlush();
        }
        logger.info("Index build of {} complete", metadata.name);
    }

    /**
     * Creates a string comprising the names of the SSTable names.
     * @param sstables the collections of tables to get the names of.
     * @return a string comprising the names of the SSTable names.
     */
    private static String getSSTableNames(Collection<SSTableReader> sstables) {
        return StreamSupport.stream(sstables.spliterator(), false).map(SSTableReader::toString)
                .collect(Collectors.joining(", "));
    }

    private static final String TRUE = "true";

    private static boolean toBoolean(final String str, boolean dflt) {
        // Previously used equalsIgnoreCase, which was fast for interned 'true'.
        // Non interned 'true' matched 15 times slower.
        //
        // Optimisation provides same performance as before for interned 'true'.
        // Similar performance for null, 'false', and other strings not length 2/3/4.
        // 'true'/'TRUE' match 4 times slower, 'tRUE'/'True' 7 times slower.
        if (str == TRUE) {
            return true;
        }
        if (str == null) {
            return dflt;
        }
        switch (str.length()) {
        case 1: {
            final char ch0 = str.charAt(0);
            if (ch0 == 'y' || ch0 == 'Y' || ch0 == 't' || ch0 == 'T' || ch0 == '1') {
                return true;
            }
            if (ch0 == 'n' || ch0 == 'N' || ch0 == 'f' || ch0 == 'F' || ch0 == '0') {
                return false;
            }
            break;
        }
        case 2: {
            final char ch0 = str.charAt(0);
            final char ch1 = str.charAt(1);
            if ((ch0 == 'o' || ch0 == 'O') && (ch1 == 'n' || ch1 == 'N')) {
                return true;
            }
            if ((ch0 == 'n' || ch0 == 'N') && (ch1 == 'o' || ch1 == 'O')) {
                return false;
            }
            break;
        }
        case 3: {
            final char ch0 = str.charAt(0);
            final char ch1 = str.charAt(1);
            final char ch2 = str.charAt(2);
            if ((ch0 == 'y' || ch0 == 'Y') && (ch1 == 'e' || ch1 == 'E') && (ch2 == 's' || ch2 == 'S')) {
                return true;
            }
            if ((ch0 == 'o' || ch0 == 'O') && (ch1 == 'f' || ch1 == 'F') && (ch2 == 'f' || ch2 == 'F')) {
                return false;
            }
            break;
        }
        case 4: {
            final char ch0 = str.charAt(0);
            final char ch1 = str.charAt(1);
            final char ch2 = str.charAt(2);
            final char ch3 = str.charAt(3);
            if ((ch0 == 't' || ch0 == 'T') && (ch1 == 'r' || ch1 == 'R') && (ch2 == 'u' || ch2 == 'U')
                    && (ch3 == 'e' || ch3 == 'E')) {
                return true;
            }
            break;
        }
        case 5: {
            final char ch0 = str.charAt(0);
            final char ch1 = str.charAt(1);
            final char ch2 = str.charAt(2);
            final char ch3 = str.charAt(3);
            final char ch4 = str.charAt(4);
            if ((ch0 == 'f' || ch0 == 'F') && (ch1 == 'a' || ch1 == 'A') && (ch2 == 'l' || ch2 == 'L')
                    && (ch3 == 's' || ch3 == 'S') && (ch4 == 'e' || ch4 == 'E')) {
                return false;
            }
            break;
        }
        default:
            break;
        }

        return dflt;
    }
}
