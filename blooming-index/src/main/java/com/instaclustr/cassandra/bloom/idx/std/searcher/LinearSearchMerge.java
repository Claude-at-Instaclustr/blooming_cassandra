package com.instaclustr.cassandra.bloom.idx.std.searcher;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.IndexKey;
import com.instaclustr.cassandra.bloom.idx.IndexMap;
import com.instaclustr.cassandra.bloom.idx.std.BFUtils;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndexSerde;

public class LinearSearchMerge extends SearchMerge {
    static final Logger logger = LoggerFactory.getLogger(LinearSearchMerge.class);

    public LinearSearchMerge( SearchMerge.Config config)
    {
        super( config );
    }


    @Override
    public SortedSet<DecoratedKey> execute() {
        /*
         * For each key in the indexKeys set, create the associated IndexMap and then
         * process each IndexKey in the IndexMap collecting the base table keys from the
         * index in a set (mapSet).
         *
         * The result is an iterator over the set of solutions for each IndexKey in
         * indexKeys.
         */
        ExtendedIterator<Set<DecoratedKey>> maps = WrappedIterator.create(config.queryKeys.iterator())
                .mapWith(IndexKey::asMap)
                .mapWith(idxMap -> {
                    logger.debug("Processing {}", idxMap);
                    Set<DecoratedKey> mapSet = new HashSet<DecoratedKey>();
                    idxMap.getKeys().mapWith(idxKey -> {
                        return UnfilteredRowIterators.filter(config.serde.read(idxKey, config.nowInSec, config.executionController),
                                config.nowInSec);
                    }).forEach(row -> {
                        WrappedIterator.create(row).mapWith(config.row2Key).forEach(mapSet::add);
                    });
                    logger.debug("Completed Returning {} entries", mapSet.size());
                    return mapSet;
                });

        /*
         * Iterate over the solutions retaining the intersection of the result solution
         * and the current solution. if the result solution becomes empty there is no
         * solution to the query and we can return.
         */
        SortedSet<DecoratedKey> result = new TreeSet<DecoratedKey>();
        if (maps.hasNext()) {
            result.addAll( maps.next() );
            while (maps.hasNext() && !result.isEmpty()) {
                result.retainAll( maps.next() );
                logger.debug("Merge yielded {} entries", result.size());
            }
        }

        return result;
    }
}
