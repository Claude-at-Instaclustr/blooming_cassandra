package com.instaclustr.cassandra.bloom.idx.std.searcher;

import com.instaclustr.cassandra.bloom.idx.std.searcher.SearchMerge.Config;

public class SearchMergeFactory {

    /**
     * Do not instantiate
     */
    private SearchMergeFactory() {
    }

    public static SearchMerge Linear( Config config ) {
        return new LinearSearchMerge( config );
    }

    public static SearchMerge Threaded( Config config ) {
        return new ThreadedSearchMerge( config );
    }

}
