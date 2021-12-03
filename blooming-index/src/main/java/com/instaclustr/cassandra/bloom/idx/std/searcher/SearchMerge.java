package com.instaclustr.cassandra.bloom.idx.std.searcher;

import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.Row;

import com.instaclustr.cassandra.bloom.idx.IndexKey;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndexSerde;

public abstract class SearchMerge {

    protected final Config config;

    /**
     * Force config constructor.
     * @param config the configuration for hte search merge.
     */
    protected SearchMerge( Config config ) {
        this.config = config;
    }

    /**
     * Returns the set of DecoratedKeys from the base table.
     * Keyps must be in natural order.
     * @return the set of DecoratedKeys from the base table.
     */
    public abstract SortedSet<DecoratedKey> execute();

    public static class Config {
        public final BloomingIndexSerde serde;
        public final Function<Row, DecoratedKey> row2Key;
        public final Set<IndexKey> queryKeys;
        public final ReadExecutionController executionController;
        public final int nowInSec;

        public Config(BloomingIndexSerde serde, Function<Row, DecoratedKey> row2Key, final Set<IndexKey> queryKeys, final ReadExecutionController executionController, int nowInSec)
        {
            this.serde=serde;
            this.row2Key = row2Key;
            this.queryKeys = queryKeys;
            this.executionController = executionController;
            this.nowInSec = nowInSec;
        }
    }
}
