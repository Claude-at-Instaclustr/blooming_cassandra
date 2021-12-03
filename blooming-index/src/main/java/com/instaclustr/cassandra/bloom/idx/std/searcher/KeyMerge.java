package com.instaclustr.cassandra.bloom.idx.std.searcher;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.CountingFilter;
import com.instaclustr.cassandra.bloom.idx.IndexKey;
import com.instaclustr.cassandra.bloom.idx.IndexMap;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndexSerde;
import com.instaclustr.cassandra.bloom.idx.std.BloomingSearcher;

public class KeyMerge implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BloomingSearcher.class);

    private final SearchMerge.Config config;
    private final IndexMap  indexMap;
    private final Consumer<Set<DecoratedKey>> callback;
    private Set<DecoratedKey> result;
    private int callbackCount = 0;
    private Executor executor;

    public KeyMerge( SearchMerge.Config config, Executor executor, Consumer<Set<DecoratedKey>> callback, IndexMap indexMap) {
        this.indexMap = indexMap;
        this.callback = callback;
        this.executor = executor;
        this.config = config;
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
                logger.debug( "Key {} callback count {}/{}", indexMap.getPosition(), callbackCount, counter.getCount());
                synchronized( this ) {
                    wait( 500 );
                }
            }
            logger.debug( "Key {} callback count {}/{}", indexMap.getPosition(), callbackCount, counter.getCount());

        } catch (AbortException e) {
            logger.error( "Processing aborted");
        } catch (InterruptedException e) {
            logger.warn( "Interrupted", e );
        } finally {
            callback.accept( result );
        }
    }

    private SearchingRunnable searcher(IndexKey key)  {
        return new SearchingRunnable( config, this::consumer, key);
    }

    public synchronized void consumer(RowIterator iter) {

        ExtendedIterator<DecoratedKey> keys = WrappedIterator.create( iter ).mapWith(config.row2Key);

        if (result == null) {
            result = keys.toSet();
            logger.trace( "Key {} initial {} keys", indexMap.getPosition(), result.size() );
        } else {
            keys.forEach( result::add );
            logger.trace( "Key {} after merge {} keys", indexMap.getPosition(), result.size() );
        }
        callbackCount++;
        this.notify();
    }
}