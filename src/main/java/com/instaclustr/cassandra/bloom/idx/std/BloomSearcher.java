package com.instaclustr.cassandra.bloom.idx.std;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.index.Index.Searcher;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.IndexKey;
import com.instaclustr.cassandra.bloom.idx.IndexMap;

public class BloomSearcher implements Searcher {
    private static final Logger logger = LoggerFactory.getLogger(CassandraIndexSearcher.class);

    private final RowFilter.Expression expression;
    private final BloomingIndexSerde serde;
    private final BloomingIndex index;
    protected final ReadCommand command;

    public BloomSearcher(BloomingIndex index, BloomingIndexSerde serde, ReadCommand command, RowFilter.Expression expression) {
        this.index = index;
        this.serde = serde;
        this.command = command;
        this.expression = expression;
    }

    @Override
    @SuppressWarnings("resource") // Both the OpOrder and 'indexIter' are closed on exception, or through the closing of the result
    // of this method.
    public UnfilteredPartitionIterator search(ReadExecutionController executionController)
    {

        Function<Row,DecoratedKey> row2Key = new Function<Row,DecoratedKey>() {

            @Override
            public DecoratedKey apply(Row hit) {
                index.baseCfs.decorateKey(hit.clustering().bufferAt(0));
            }};

        // generate the list of records to retrieve from the index
        Set<IndexKey> lst = new TreeSet<IndexKey>();

        BFUtils.getIndexKeys(expression.getIndexValue())
        .filterDrop( IndexKey::isZero ).forEach( lst::add );


        ExtendedIterator<Set<DecoratedKey>> maps = WrappedIterator.create( lst.iterator() )
                .mapWith( IndexKey::asMap )
                .mapWith( idxMap -> {
            Set<DecoratedKey> mapSet = new HashSet<DecoratedKey>();
            Iterator<RowIterator>  rows = idxMap.getKeys()
                    .mapWith( idxKey -> {
                return UnfilteredRowIterators.filter(serde.read(idxKey, command, executionController), command.nowInSec());} );
            while (rows.hasNext() ) {
                WrappedIterator.create(rows.next())
                        .mapWith(row2Key).forEach( mapSet::add );
            }
            return mapSet;
                } );
        Set<DecoratedKey> result = null;
            if (!maps.hasNext()) {
                result = Collections.emptySet();
            } else {
                result = maps.next();
                while (maps.hasNext()) {
                    result.retainAll( maps.next() );
                }
            }
            // END OF EDIT
            // TODO perform querydatafromindex style query to return only the records in result.
        }

        // the value of the index expression is the partition key in the index table


        try
        {
            return queryDataFromIndex(indexKey, UnfilteredRowIterators.filter(indexIter, command.nowInSec()), command, executionController);
        }
        catch (RuntimeException | Error e)
        {
            indexIter.close();
            throw e;
        }
    }



    /**
     * Specific to internal indexes, this is called by a
     * searcher when it encounters a stale entry in the index
     * @param indexKey the partition key in the index table
     * @param indexClustering the clustering in the index table
     * @param deletion deletion timestamp etc
     * @param ctx the write context under which to perform the deletion
     */
    public void deleteStaleEntry(DecoratedKey indexKey, Clustering<?> indexClustering, DeletionTime deletion,
            WriteContext ctx) {
        serde.doDelete(indexKey, indexClustering, deletion, ctx);
        logger.trace("Removed index entry for stale value {}", indexKey);
    }

    private UnfilteredPartitionIterator queryDataFromIndex(final DecoratedKey indexKey, final RowIterator indexHits,
            final ReadCommand command, final ReadExecutionController executionController) {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

        return new UnfilteredPartitionIterator() {
            private UnfilteredRowIterator next;

            @Override
public TableMetadata metadata() {
                return command.metadata();
            }

            @Override
public boolean hasNext() {
                return prepareNext();
            }

            @Override
public UnfilteredRowIterator next() {
                if (next == null)
                    prepareNext();

                UnfilteredRowIterator toReturn = next;
                next = null;
                return toReturn;
            }

            private boolean prepareNext() {
                while (next == null && indexHits.hasNext()) {
                    Row hit = indexHits.next();
                    DecoratedKey key = index.baseCfs.decorateKey(hit.clustering().bufferAt(0));
                    if (!command.selectsKey(key))
                        continue;

                    ColumnFilter extendedFilter = getExtendedFilter(command.columnFilter());
                    SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                            command.nowInSec(), extendedFilter, command.rowFilter(), DataLimits.NONE, key,
                            command.clusteringIndexFilter(key), null);

                    @SuppressWarnings("resource") // filterIfStale closes it's iterator if either it materialize it or
                                                  // if it returns null.
                    // Otherwise, we close right away if empty, and if it's assigned to next it will
                    // be called either
                    // by the next caller of next, or through closing this iterator is this come
                    // before.
                    UnfilteredRowIterator dataIter = filterIfStale(
                            dataCmd.queryMemtableAndDisk(index.baseCfs, executionController), hit, indexKey.getKey(),
                            executionController.getWriteContext(), command.nowInSec());

                    if (dataIter != null) {
                        if (dataIter.isEmpty())
                            dataIter.close();
                        else
                            next = dataIter;
                    }
                }
                return next != null;
            }

            @Override
public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
public void close() {
                indexHits.close();
                if (next != null)
                    next.close();
            }
        };
    }

}
