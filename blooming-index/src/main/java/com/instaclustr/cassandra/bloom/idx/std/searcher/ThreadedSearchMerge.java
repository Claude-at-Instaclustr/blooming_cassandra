package com.instaclustr.cassandra.bloom.idx.std.searcher;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.IndexKey;
import com.instaclustr.cassandra.bloom.idx.IndexMap;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndexSerde;
import com.instaclustr.cassandra.bloom.idx.std.BloomingSearcher;

public class ThreadedSearchMerge extends SearchMerge implements Consumer<Set<DecoratedKey>> {
    static final Logger logger = LoggerFactory.getLogger(ThreadedSearchMerge.class);


    private SortedSet<DecoratedKey> result = null;
    private int callbackCount = 0;
    private boolean errorDetected = false;

    private ExecutorService executor;

    public ThreadedSearchMerge( SearchMerge.Config config )
    {
        super(config);
    }

    @Override
    public SortedSet<DecoratedKey> execute() {

        try {
            executor = Executors.newFixedThreadPool(config.queryKeys.size()*2);
            WrappedIterator.create(config.queryKeys.iterator()).mapWith(IndexKey::asMap)
            .mapWith( this::keyMerge )
            .forEach( executor::execute );

            while (callbackCount < config.queryKeys.size() && !errorDetected) {
                logger.debug( "Search callback count {}/{}", callbackCount, config.queryKeys.size());
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
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }

        return result;
    }

    KeyMerge keyMerge(IndexMap map)  {
        return new KeyMerge( config, executor, this::accept, map );
    }

    @Override
    public synchronized void accept(Set<DecoratedKey> keys) {
        if (keys == null) {
            this.errorDetected = true;
        } else {
            if (result != null) {
                result.retainAll( keys );
                logger.debug( "After intersection {} keys", result.size() );
            } else {
                // results must be in proper order
                result = new TreeSet<DecoratedKey>();
                result.addAll( keys );
            }
        }
        ++callbackCount;
        this.notify();
    }

}