package com.instaclustr.cassandra.bloom.idx.mem.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.awaitility.Awaitility.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static java.util.concurrent.TimeUnit.*;
import org.apache.cassandra.io.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.Func;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.OutputTimeoutException;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.RangeLock;

public class MultiThreadedTests {

    private static File dir;
    private File file;
    private ExecutorService executor;

    public MultiThreadedTests() {
        executor = Executors.newFixedThreadPool(3);
    }

    @BeforeClass
    public static void beforeClass() {
        dir = Files.createTempDir();
    }

    public static void afterClass() {
        FileUtils.deleteRecursive(dir);
    }

    @Before
    public void setup() {
        file = new File(dir, "BaseTable");
    }

    @After
    public void teardown() {
        FileUtils.delete(file);
    }

    @Test
    public void getRangeLockTest() throws IOException, InterruptedException {
        List<Integer> lst = new ArrayList<Integer>();
        try (BaseTable table = new BaseTable(file, 5)) {
            table.getLockedBlocks(lst::add);
            assertEquals(0, lst.size());
            lst.clear();

            Callable<Boolean> getLock = new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    try (RangeLock rangeLock = table.getLock(0, 5, 1)) {
                        return rangeLock.hasLock();
                    }
                }
            };

            // nested locks work
            try (RangeLock rangeLock = table.getLock(0, 5, 1)) {
                assertTrue(rangeLock.hasLock());
                RangeLock rl2 = table.getLock(0, 5, 1);
                assertTrue(rl2.hasLock());
                rl2.close();

                table.getLockedBlocks(lst::add);
                assertEquals(1, lst.size());
            }

            // non nested locs do not
            try (RangeLock rangeLock = table.getLock(0, 5, 1)) {
                try {
                    executor.submit(getLock).get();
                    fail("Should have thrown execption");
                } catch (ExecutionException e) {
                    assertEquals(OutputTimeoutException.class, e.getCause().getClass());
                }
            }

            // lock after release works.
            try {
                assertTrue(executor.submit(getLock).get());
            } catch (InterruptedException | ExecutionException e) {
                fail("Should not have thrown exception");
            }
        }
    }

    @Test
    public void getRangeLockOverlappingTest() throws IOException, InterruptedException {
        List<Integer> lst = new ArrayList<Integer>();
        try (BaseTable table = new BaseTable(file, 5)) {
            table.getLockedBlocks(lst::add);
            assertEquals(0, lst.size());
            lst.clear();

            Callable<Boolean> getLock = new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    try (RangeLock rangeLock = table.getLock(5, 10, 1)) {
                        return rangeLock.hasLock();
                    }
                }
            };

            // nested locks work
            try (RangeLock rangeLock = table.getLock(0, 10, 1)) {
                assertTrue(rangeLock.hasLock());
                RangeLock rl2 = table.getLock(5, 10, 1);
                assertTrue(rl2.hasLock());
                rl2.close();

                table.getLockedBlocks(lst::add);
                assertEquals(2, lst.size());
            }

            // non nested locs do not
            try (RangeLock rangeLock = table.getLock(0, 10, 1)) {
                try {
                    executor.submit(getLock).get();
                    fail("Should have thrown execption");
                } catch (ExecutionException e) {
                    assertEquals(OutputTimeoutException.class, e.getCause().getClass());
                }
            }

            // lock after release works.
            try {
                assertTrue(executor.submit(getLock).get());
            } catch (InterruptedException | ExecutionException e) {
                fail("Should not have thrown exception");
            }
        }
    }

    @Test
    public void getRangeLockAdjacentTest() throws IOException, InterruptedException {
        List<Integer> lst = new ArrayList<Integer>();
        try (BaseTable table = new BaseTable(file, 5)) {
            table.getLockedBlocks(lst::add);
            assertEquals(0, lst.size());
            lst.clear();

            Callable<Boolean> getLock = new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    try (RangeLock rangeLock = table.getLock(5, 5, 1)) {
                        return rangeLock.hasLock();
                    }
                }
            };

            // nested locks work
            try (RangeLock rangeLock = table.getLock(0, 5, 1)) {
                assertTrue(rangeLock.hasLock());
                RangeLock rl2 = table.getLock(5, 5, 1);
                assertTrue(rl2.hasLock());
                rl2.close();

                table.getLockedBlocks(lst::add);
                assertEquals(1, lst.size());
                assertEquals(2, table.getLockCount());
            }

            // non nested locks do work
            try (RangeLock rangeLock = table.getLock(0, 5, 1)) {
                try {
                    assertTrue(executor.submit(getLock).get());
                } catch (ExecutionException e) {
                    fail("Should not have thrown exception");
                }
            }

            // lock after release works.
            Future<Boolean> future = executor.submit(getLock);
            try {
                assertTrue(executor.submit(getLock).get());
            } catch (InterruptedException | ExecutionException e) {
                fail("Should not have thrown exception");
            }
        }
    }

    @Test
    public void syncTest() throws IOException {
        class LockHolder implements Func {
            boolean hold = true;
            boolean running = false;

            boolean isRunning() {
                return running;
            }

            @Override
            public void call() throws Exception {
                running = true;
                while (hold) {
                    Thread.yield();
                }
                running = false;
            }

        }

        try (BaseTable table = new BaseTable(file, 5)) {

            class RunnableLockHolder implements Callable<Void> {
                LockHolder lockHolder;
                int count;

                RunnableLockHolder(LockHolder lockHolder, int count) {
                    this.lockHolder = lockHolder;
                    this.count = count;
                }

                @Override
                public Void call() throws Exception {
                    table.sync(lockHolder, 0, 5, count);
                    return null;
                }

            }

            LockHolder one = new LockHolder();
            LockHolder two = new LockHolder();

            RunnableLockHolder rOne = new RunnableLockHolder(one, 0);
            RunnableLockHolder rTwo = new RunnableLockHolder(two, 120); // one minute

            Future<?> future1 = executor.submit(rOne);
            Future<?> future2 = executor.submit(rTwo);

            await("One not started").atMost(1, SECONDS).until(one::isRunning);

            assertFalse(future1.isDone());
            assertFalse(future2.isDone());
            assertFalse(two.running);
            one.hold = false;

            await("Two not started").atMost(1, SECONDS).until(two::isRunning);

            assertTrue(future1.isDone());
            assertFalse(one.running);
            assertFalse(future2.isDone());
            two.hold = false;

            await("Two not stopped").atMost(1, SECONDS).until(() -> {
                return !two.running;
            });

            await("Two not done").atMost(1, SECONDS).until(future2::isDone);
        }

    }
}
