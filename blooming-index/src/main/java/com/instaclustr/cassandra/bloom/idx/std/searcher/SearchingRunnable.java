package com.instaclustr.cassandra.bloom.idx.std.searcher;

import java.util.function.Consumer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.IndexKey;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndexSerde;
import com.instaclustr.cassandra.bloom.idx.std.BloomingSearcher;

class SearchingRunnable implements Runnable {
    static final Logger logger = LoggerFactory.getLogger(BloomingSearcher.class);

    /**
     *
     */
    private final SearchMerge.Config config;
    private final Consumer<RowIterator> callback;
    private final IndexKey indexKey;

    public SearchingRunnable(SearchMerge.Config config, Consumer<RowIterator> callback, IndexKey indexKey)
    {
        this.config = config;
        this.callback = callback;
        this.indexKey = indexKey;
    }

    @Override
    public void run() {
        try (RowIterator iter = UnfilteredRowIterators.filter(config.serde.read(indexKey, config.nowInSec, config.executionController), config.nowInSec )){
            callback.accept( iter );
        }

    }
}