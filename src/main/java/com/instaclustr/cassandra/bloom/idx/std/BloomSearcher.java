package com.instaclustr.cassandra.bloom.idx.std;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.index.Index.Searcher;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BloomSearcher implements Searcher {
    private static final Logger logger = LoggerFactory.getLogger(CassandraIndexSearcher.class);

    private final RowFilter.Expression expression;
    protected final BloomingIndex index;
    protected final ReadCommand command;

    public BloomSearcher(ReadCommand command, RowFilter.Expression expression , BloomingIndex index) {
        this.command = command;
        this.expression = expression;
        this.index = index;    }

    @Override
    @SuppressWarnings("resource") // Both the OpOrder and 'indexIter' are closed on exception, or through the closing of the result
    // of this method.
    public UnfilteredPartitionIterator search(ReadExecutionController executionController)
    {
        // the value of the index expression is the partition key in the index table
        DecoratedKey indexKey = index.getBackingTable().get().decorateKey(expression.getIndexValue());
        UnfilteredRowIterator indexIter = queryIndex(indexKey, command, executionController);
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

}
