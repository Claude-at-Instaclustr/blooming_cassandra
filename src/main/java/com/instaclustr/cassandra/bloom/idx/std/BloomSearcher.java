package com.instaclustr.cassandra.bloom.idx.std;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.IndexKey;

public class BloomSearcher implements Searcher {
    private static final Logger logger = LoggerFactory.getLogger(CassandraIndexSearcher.class);

    private final RowFilter.Expression expression;
    private final BloomingIndexSerde serde;
    private final BloomingIndex index;
    protected final ReadCommand command;

    private Function<Row, DecoratedKey> row2Key;

    public BloomSearcher(BloomingIndex index, BloomingIndexSerde serde, ReadCommand command,
            RowFilter.Expression expression) {
        this.index = index;
        this.serde = serde;
        this.command = command;
        this.expression = expression;
        this.row2Key = new Function<Row, DecoratedKey>() {

            @Override
            public DecoratedKey apply(Row hit) {
                return index.baseCfs.decorateKey(hit.clustering().bufferAt(0));
            }
        };
    }

    @Override
    @SuppressWarnings("resource") // Both the OpOrder and 'indexIter' are closed on exception, or through the
    // closing of the result
    // of this method.
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) {
        Function<DecoratedKey, UnfilteredRowIterator> key2RowIter = new Function<DecoratedKey, UnfilteredRowIterator>() {

            @Override
            public UnfilteredRowIterator apply(DecoratedKey hit) {
                ColumnFilter extendedFilter = getExtendedFilter(command.columnFilter());
                SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                        command.nowInSec(), extendedFilter, command.rowFilter(), DataLimits.NONE, hit,
                        command.clusteringIndexFilter(hit), null);
                return dataCmd.queryMemtableAndDisk(index.baseCfs, executionController);
            };
        };

        // generate the list of records to retrieve from the index
        Set<IndexKey> lst = new TreeSet<IndexKey>();
        BFUtils.getIndexKeys(expression.getIndexValue()).filterDrop(IndexKey::isZero).forEach(lst::add);

        ExtendedIterator<Set<DecoratedKey>> maps = WrappedIterator.create(lst.iterator()).mapWith(IndexKey::asMap)
                .mapWith(idxMap -> {
                    Set<DecoratedKey> mapSet = new HashSet<DecoratedKey>();
                    Iterator<RowIterator> rows = idxMap.getKeys().mapWith(idxKey -> {
                        return UnfilteredRowIterators.filter(serde.read(idxKey, command, executionController),
                                command.nowInSec());
                    });
                    while (rows.hasNext()) {

                        WrappedIterator.create(rows.next()).mapWith(row2Key).forEach(mapSet::add);
                    }
                    return mapSet;
                });
        Set<DecoratedKey> result = null;
        if (!maps.hasNext()) {
            result = Collections.emptySet();
        } else {
            result = maps.next();
            while (maps.hasNext()) {
                result.retainAll(maps.next());
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
