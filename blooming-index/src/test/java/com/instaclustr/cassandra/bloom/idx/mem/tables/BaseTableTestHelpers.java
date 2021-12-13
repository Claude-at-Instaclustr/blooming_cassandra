package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

public class BaseTableTestHelpers {

    /**
     * Verifies that no locks are left
     * @param tbl the table to check
     */
    public static void assertNoLocks(BaseTable tbl) {
        List<Integer> lst = new ArrayList<Integer>();
        tbl.getLockedBlocks(lst::add);
        lst.forEach((i) -> System.out.println(String.format("Block %s is locked", i)));
        assertTrue(lst.isEmpty());
    }

}
