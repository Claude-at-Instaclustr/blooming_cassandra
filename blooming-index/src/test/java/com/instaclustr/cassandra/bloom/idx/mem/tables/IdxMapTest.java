package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTableTestHelpers.assertNoLocks;
import static org.junit.Assert.assertEquals;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.tables.IdxMap.MapEntry;

public class IdxMapTest {

    private static File dir;
    private File file;

    @BeforeClass
    public static void beforeClass() {
        dir = Files.createTempDir();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        FileUtils.deleteDirectory(dir);
    }

    @Before
    public void setup() {
        file = new File(dir, "idxTable");
    }

    @After
    public void teardown() throws IOException {
        FileUtils.delete(file);
    }

    @Test
    public void getTest() throws IOException {
        try (IdxMap idxMap = new IdxMap(file)) {
            idxMap.get(0);
            idxMap.get(1);
            assertNoLocks(idxMap);
        }
    }

    @Test
    public void setValuesTest() throws IOException {
        try (IdxMap idxMap = new IdxMap(file)) {
            MapEntry entry0 = idxMap.get(5);
            MapEntry entry1 = idxMap.get(5);

            entry0.setKeyIdx(5);

            assertEquals(5, entry1.getKeyIdx());
            assertNoLocks(idxMap);
        }
    }

}
