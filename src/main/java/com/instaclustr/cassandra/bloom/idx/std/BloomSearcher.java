package com.instaclustr.cassandra.bloom.idx.std;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.cassandra.schema.TableMetadata;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.assertj.core.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.IndexKey;

public class BloomSearcher implements Searcher {
    private static final Logger logger = LoggerFactory.getLogger(CassandraIndexSearcher.class);

    private final RowFilter.Expression expression;
    private final BloomingIndexSerde serde;
    private final BloomingIndex index;
    private final ReadCommand command;

    private Function<Row, DecoratedKey> row2Key;

    public BloomSearcher(BloomingIndex index, BloomingIndexSerde serde, ReadCommand command,
            RowFilter.Expression expression) {
        this.index = index;
        this.serde = serde;
        this.command = command;
        this.expression = expression;

        // A function to convert a row to the key
        this.row2Key = new Function<Row, DecoratedKey>() {

            @Override
            public DecoratedKey apply(Row hit) {
                if (logger.isDebugEnabled()) {
                    ByteBuffer bb =  hit.clustering().bufferAt(0);
                    StringBuilder sb = new StringBuilder("Reading ");
                    while (bb.hasRemaining() ) {
                        sb.append( (char) bb.get() );
                    }
                    logger.debug(sb.toString());
                }
                return index.getBaseCfs().decorateKey(hit.clustering().bufferAt(0));
            }
        };
    }

    @Override
    @SuppressWarnings("resource") // Both the OpOrder and 'indexIter' are closed on exception, or through the
    // closing of the result
    // of this method.
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) {
        // Create a function to convert DecoratedKey from the index to a Row from the
        // base table.
        Function<DecoratedKey, UnfilteredRowIterator> key2RowIter = new Function<DecoratedKey, UnfilteredRowIterator>() {

            @Override
            public UnfilteredRowIterator apply(DecoratedKey hit) {
                ColumnFilter extendedFilter = getExtendedFilter(command.columnFilter());
                SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(index.getBaseCfs().metadata(),
                        command.nowInSec(), extendedFilter, command.rowFilter(), DataLimits.NONE, hit,
                        command.clusteringIndexFilter(hit), null);
                return dataCmd.queryMemtableAndDisk(index.getBaseCfs(), executionController);
            };
        };

        // generate the sorted set of IndexKeys for records to retrieve from the index
        Set<IndexKey> lst = new TreeSet<IndexKey>();
        BFUtils.getIndexKeys(expression.getIndexValue()).forEach(lst::add);

        /*
         * For each key in the index set, create the associated IndexMap and then
         * process each IndexKey in the IndexMap collecting the base table key values
         * from the index in a set (mapSet).
         *
         * The result is an iterator over the set of solutions for each IndexKey in lst.
         */
        ExtendedIterator<Set<DecoratedKey>> maps = WrappedIterator.create(lst.iterator())
                .mapWith(IndexKey::asMap)
                .mapWith(idxMap -> {
                    logger.debug( "Processing {}", idxMap );
                    Set<DecoratedKey> mapSet = new HashSet<DecoratedKey>();
                    idxMap.getKeys()
                        .mapWith(idxKey -> {
                            return UnfilteredRowIterators.filter(serde.read(idxKey, command, executionController),
                                command.nowInSec());
                            })
                        .forEach(row -> {
                            WrappedIterator.create(row).mapWith(row2Key).forEach(mapSet::add);
                            });
                    logger.debug( "Completed Returning {} entries", mapSet.size());
                    return mapSet;
                });

        /*
         * Iterate over the solutions retaining the intersection of the result solution
         * and the current solution.  if the result solution becomes empty there is no solution
         * to the query and we can return.
         */
        Set<DecoratedKey> result = null;
        if (!maps.hasNext()) {
            result = Collections.emptySet();
        } else {
            result = maps.next();
            while (maps.hasNext() && !result.isEmpty()) {
                Set<DecoratedKey> nxt = maps.next();
                result.retainAll(nxt);
                logger.debug( "Merge yielded {} entries", result.size());
            }
        }

        return createUnfilteredPartitionIterator(WrappedIterator.create(result.iterator()).mapWith(key2RowIter));

    }

    private ColumnFilter getExtendedFilter(ColumnFilter initialFilter) {
        if (command.columnFilter().fetches(index.getIndexedColumn()))
            return initialFilter;

        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        builder.addAll(initialFilter.fetchedColumns());
        builder.add(index.getIndexedColumn());
        return builder.build();
    }

    private UnfilteredPartitionIterator createUnfilteredPartitionIterator(
            ExtendedIterator<UnfilteredRowIterator> a_rowIter) {

        return new UnfilteredPartitionIterator() {
            ExtendedIterator<UnfilteredRowIterator> rowIter = a_rowIter;

            @Override
            public TableMetadata metadata() {
                return command.metadata();
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

}
