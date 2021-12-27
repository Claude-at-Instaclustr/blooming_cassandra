package com.instaclustr.cassandra.bloom.idx;

import java.util.function.Predicate;

/**
 * Predicate that counts items.  All items are accepted.
 *
 * @param <T> The class being tested
 */
public class CountingPredicate<T> implements Predicate<T> {

    /**
     * the counter.
     */
    private int count = 0;

    @Override
    public boolean test(T arg) {
        count++;
        return true;
    }

    /**
     * Get the counts after usage.
     * @return the number of items tested.
     */
    public int getCount() {
        return count;
    }

}
