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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.index.Index.Searcher;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.CountingFilter;
import com.instaclustr.cassandra.bloom.idx.IndexKey;
import com.instaclustr.cassandra.bloom.idx.IndexMap;

/**
 * Handles searching the index for mathing filters.
 */
public class BloomingSearcher implements Searcher {
    private static final Logger logger = LoggerFactory.getLogger(BloomingSearcher.class);

    /**
     * The expression to use for the search.
     */
    private final RowFilter.Expression expression;
    /**
     * The Serde for index I/O.
     */
    private final BloomingIndexSerde serde;
    /**
     * The read command being executed.
     */
    private final ReadCommand command;
    /**
     * the indexed column in the base table.
     */
    private final ColumnMetadata indexedColumn;
    /**
     * the base table.
     */
    private final ColumnFamilyStore baseCfs;

    /**
     * A function to convert a row from the base table into the a key for the row.
     */
    private final Function<Row, DecoratedKey> row2Key;

    private final Executor executor;

    /**
     * Constructor.
     * @param indexedColumn the indexed column in the base table.
     * @param baseCfs the base table.
     * @param serde The Serde for index I/O.
     * @param command The read command being executed.
     * @param expression The expression to use for the search.
     */
    public BloomingSearcher(final ColumnMetadata indexedColumn, final ColumnFamilyStore baseCfs,
            final BloomingIndexSerde serde, final ReadCommand command, final RowFilter.Expression expression) {
        this.baseCfs = baseCfs;
        this.indexedColumn = indexedColumn;
        this.serde = serde;
        this.command = command;
        this.expression = expression;
        this.executor = Executors.newFixedThreadPool(5);


        // A function to convert a serde row to the base key
        this.row2Key = new Function<Row, DecoratedKey>() {

            @Override
            public DecoratedKey apply(Row hit) {
                if (logger.isTraceEnabled()) {
                    ByteBuffer bb = hit.clustering().bufferAt(0);
                    StringBuilder sb = new StringBuilder("Reading ");
                    while (bb.hasRemaining()) {
                        sb.append((char) bb.get());
                    }
                    logger.trace(sb.toString());
                }
                return baseCfs.decorateKey(hit.clustering().bufferAt(0));
            }
        };
    }

    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) {


        // Create a function to convert DecoratedKey from the index to a Row from the
        // base table.
        Function<DecoratedKey, UnfilteredRowIterator> key2RowIter = new Function<DecoratedKey, UnfilteredRowIterator>() {

            @Override
            public UnfilteredRowIterator apply(DecoratedKey hit) {
                ColumnFilter extendedFilter = getExtendedFilter(command.columnFilter());
                SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(baseCfs.metadata(),
                        command.nowInSec(), extendedFilter, command.rowFilter(), DataLimits.NONE, hit,
                        command.clusteringIndexFilter(hit), null);
                return dataCmd.queryMemtableAndDisk(baseCfs, executionController);
            };
        };

        // generate the sorted set of IndexKeys for the Bloom filter specified in the
        // query
        Set<IndexKey> queryKeys = new TreeSet<IndexKey>();
        BFUtils.getIndexKeys(expression.getIndexValue()).forEach(queryKeys::add);

        /*
         * For each key in the queryKeys set, create the associated IndexMap and then
         * process each IndexKey in the IndexMap collecting the base table keys from the
         * index in a set (mapSet).
         *
         * The result is an iterator over the set of solutions for each IndexKey in
         * queryKeys.
         */
        SearchMerge merger = new SearchMerge(queryKeys, executionController );
        Set<DecoratedKey> result = merger.execute();

//        ExtendedIterator<Set<DecoratedKey>> maps = WrappedIterator.create(queryKeys.iterator()).mapWith(IndexKey::asMap)
//                .mapWith(idxMap -> {
//                    logger.debug("Processing {}", idxMap);
//
//                    idxMap.getKeys().mapWith(idxKey -> {
//                        return UnfilteredRowIterators.filter(serde.read(idxKey, command.nowInSec(), executionController),
//                                command.nowInSec());
//                    }).forEach(row -> {
//                        WrappedIterator.create(row).mapWith(row2Key).forEach(mapSet::add);
//                    });
//                    logger.debug("Completed Returning {} entries", mapSet.size());
//                    return mapSet;
//                });



        // return a PartitionIterator that contains all the results.
        return createUnfilteredPartitionIterator(WrappedIterator.create(result.iterator()).mapWith(key2RowIter),
                command.metadata());

    }

    /**
     * Ensures that the ColumnFilter includes the indexed column.
     * @param initialFilter the filter to check.
     * @return A ColumnFilter that includes the indexed column.
     */
    private ColumnFilter getExtendedFilter(ColumnFilter initialFilter) {
        if (initialFilter.fetches(indexedColumn))
            return initialFilter;

        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        builder.addAll(initialFilter.fetchedColumns());
        builder.add(indexedColumn);
        return builder.build();
    }

    /**
     * Create an UnfilteredPartitionIterator from an UnfilteredRowIterator.
     *
     * @param theRowIterator the unfilteredRowIterator to use.
     * @return an UnfilteredPartitionIterator that will return the rows from the UnfilteredRowIterator.
     */
    private static UnfilteredPartitionIterator createUnfilteredPartitionIterator(
            ExtendedIterator<UnfilteredRowIterator> theRowIterator, TableMetadata theMetadata) {

        return new UnfilteredPartitionIterator() {
            ExtendedIterator<UnfilteredRowIterator> rowIter = theRowIterator;
            TableMetadata metadata = theMetadata;

            @Override
            public TableMetadata metadata() {
                return metadata;
            }

            @Override
            public boolean hasNext() {
                return rowIter.hasNext();
            }

            @Override
            public UnfilteredRowIterator next() {
                return rowIter.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                rowIter.close();
            }
        };
    }

    private class KeyMerge implements Runnable {
        private final IndexMap  indexMap;
        private final ReadExecutionController executionController;
        private final Consumer<Set<DecoratedKey>> callback;
        private final Semaphore permit;
        private Set<DecoratedKey> result;
        private int callbackCount = 0;

        public KeyMerge(Semaphore permit, Consumer<Set<DecoratedKey>> callback, IndexMap indexMap,ReadExecutionController executionController) {
            this.indexMap = indexMap;
            this.executionController = executionController;
            this.callback = callback;
            this.permit = permit;
        }

        @Override
        public void run() {
            CountingFilter<IndexKey> counter = new CountingFilter<IndexKey>();
            logger.debug("Processing {}", indexMap);

            try {
                indexMap.getKeys()
                    .filterKeep( counter )
                    .mapWith( this::searcher )
                    .forEach( executor::execute );

                while (callbackCount < counter.getCount()) {
                    synchronized( this ) {
                        wait( 500 );
                    }
                }


            } catch (AbortException e) {
                logger.error( "Processing aborted");
            } catch (InterruptedException e) {
                logger.warn( "Interrupted", e );
            } finally {
                callback.accept( result );
            }
        }

        private SearchingRunnable searcher(IndexKey key)  {
            try {
                permit.acquire();
            } catch (InterruptedException e) {
                throw new AbortException();
            }
            return new SearchingRunnable( this::consumer, key, executionController );
        }

        public synchronized void consumer(ExtendedIterator<DecoratedKey> iter) {
            if (result == null) {
                result = iter.toSet();
                logger.debug( "initial {} keys", result.size() );
            } else {
                iter.forEach( result::add );
                logger.debug( "after merge {} keys", result.size() );
            }
            callbackCount++;
            permit.release();
        }
    }

    private class SearchingRunnable implements Runnable {
        private final Consumer<ExtendedIterator<DecoratedKey>> callback;
        private final IndexKey indexKey;
        private final ReadExecutionController executionController;
        private int nowInSec;

        SearchingRunnable(Consumer<ExtendedIterator<DecoratedKey>> callback, IndexKey indexKey, ReadExecutionController executionController)
        {
            this.callback = callback;
            this.indexKey = indexKey;
            this.executionController = executionController;
        }

        @Override
        public void run() {
            RowIterator iter = UnfilteredRowIterators.filter(serde.read(indexKey, nowInSec, executionController),
                        nowInSec );
            callback.accept( WrappedIterator.create( iter ).mapWith(row2Key) );

        }
    }

    private class SearchMerge implements Consumer<Set<DecoratedKey>> {

        private final Set<IndexKey> indexKeys;
        private final ReadExecutionController executionController;
        private final Semaphore permit;

        private Set<DecoratedKey> result = null;
        private int callbackCount = 0;
        private boolean errorDetected = false;

        public SearchMerge( final Set<IndexKey> indexKeys, final ReadExecutionController executionController)
        {
            this.indexKeys = indexKeys;
            this.executionController = executionController;
            this.permit  = new Semaphore(20);
        }


        public Set<DecoratedKey> execute() {

            try {
                WrappedIterator.create(indexKeys.iterator()).mapWith(IndexKey::asMap)
                .mapWith( this::keyMerge )
                .forEach( executor::execute );

                while (callbackCount < indexKeys.size() && !errorDetected) {
                    synchronized( this ) {
                        try {
                            wait( 500 );
                        } catch (InterruptedException e) {
                            logger.warn( "Interrupted", e );
                        }
                    }
                }
                logger.debug("Completed Returning {} entries", result.size());
            } catch (AbortException e) {
                logger.warn("Processing Aborted -- Returning {} entries", result.size() );
            }
            return result;
        }

        KeyMerge keyMerge(IndexMap map)  {
            try {
                permit.acquire();
            } catch (InterruptedException e) {
                throw new AbortException();
            }
            return new KeyMerge( permit, this::accept, map, executionController );
        }

        @Override
        public synchronized void accept(Set<DecoratedKey> keys) {
            if (keys == null) {
                this.errorDetected = true;
            } else {
                if (result != null) {
                    result.retainAll( keys );
                } else {
                    result = keys;
                }
            }
            permit.release();
            this.notify();
        }

    }

    private class AbortException extends RuntimeException {}

}
