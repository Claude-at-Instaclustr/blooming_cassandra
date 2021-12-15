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

    // @Test
    // public void reuseTest() throws IOException {
    // try (IdxTable idxTable = new IdxTable(file)) {
    // for (int i = 0; i < 10; i++) {
    // assertEquals(i, idxTable.newIndex());
    // }
    //
    // idxTable.clear(5);
    //
    // assertEquals(5, idxTable.newIndex());
    // }
    // }
    //
    // @Test
    // public void cardinalityTest() throws IOException {
    // try (IdxTable idxTable = new IdxTable(file)) {
    // for (int i = 0; i < 10; i++) {
    // assertEquals(i, idxTable.newIndex());
    // }
    //
    // assertEquals(10, idxTable.cardinality());
    // idxTable.clear(5);
    // assertEquals(9, idxTable.cardinality());
    // }
    // }
    //
    // @Test
    // public void isSetTest() throws IOException {
    // try (IdxTable idxTable = new IdxTable(file)) {
    // for (int i = 0; i < 10; i++) {
    // assertEquals(i, idxTable.newIndex());
    // }
    //
    // assertTrue(idxTable.isSet(5));
    // idxTable.clear(5);
    // assertFalse(idxTable.isSet(5));
    // }
    // }
    //
    // @Test
    // public void isSetNotWrittenTest() throws IOException {
    // try (IdxTable idxTable = new IdxTable(file)) {
    // assertFalse(idxTable.isSet(5));
    // }
    // }
    //
    // // base for test callables
    // abstract class B implements Callable<Boolean> {
    // public boolean running = true;
    // final IdxTable idxTable;
    // final ExecutorService executor;
    // boolean ranOnce = false;
    // int idx;
    // Callable<Boolean> isSet=new Callable<Boolean>(){@Override public Boolean
    // call()throws IOException{return idxTable.isSet(idx);}};
    // private final int id;
    //
    // B(int id, IdxTable idxTable, ExecutorService executor) {
    // this.id = id;
    // this.idxTable = idxTable;
    // this.executor = executor;
    // }
    //
    // @Override
    // public String toString() {
    // return this.getClass().getSimpleName() + "-" + id;
    // }
    // }
    //
    // // reader callable
    // class R extends B {
    // public boolean status;
    //
    // R(int id, IdxTable idxTable, ExecutorService executor, int pos) {
    // super( id, idxTable, executor );
    // this.idx = pos;
    // }
    //
    // @Override
    // public Boolean call() {
    // try {
    // while (running) {
    // status = executor.submit(isSet).get(1, TimeUnit.SECONDS);
    // while (running && (status == executor.submit(isSet).get(1,
    // TimeUnit.SECONDS))) {
    // ranOnce = true;
    // Thread.yield();
    // }
    // }
    // } catch (InterruptedException | ExecutionException | TimeoutException e) {
    // System.out.println( String.format("%s watching position %s has %s", this,
    // idx, e));
    // e.printStackTrace();
    // fail();
    // }
    // return ranOnce;
    // }
    // }
    //
    // // writer callable
    // class W extends B {
    //
    // int loops;
    // IntConsumer consumer;
    //
    // Callable<Object> clear=new Callable<Object>(){@Override public Object
    // call()throws IOException{idxTable.clear(idx);return"";}};
    //
    // W(int id, IdxTable idxTable, ExecutorService executor, int loops, IntConsumer
    // consumer) {
    // super( id, idxTable, executor );
    // this.loops = loops;
    // this.consumer = consumer;
    // }
    //
    // @Override
    // public Boolean call() {
    // while (loops > 0) {
    // try {
    // idx = executor.submit(idxTable::newIndex).get(1, TimeUnit.SECONDS);
    // consumer.accept(idx);
    // Thread.sleep(40);
    // // since we created the idx nobody else should be able to disable it
    // if (! idxTable.isSet(idx)) {
    // System.out.println( String.format("%s idx %s was reset while we held it",
    // this, idx));
    // fail();
    // }
    //
    // executor.submit(clear).get(1, TimeUnit.SECONDS);
    // Thread.sleep(40);
    // ranOnce = true;
    // loops--;
    // } catch (TimeoutException | InterruptedException | ExecutionException |
    // IOException e) {
    // System.out.println( String.format("%s exception writing: %s", this, e));
    // e.printStackTrace();
    // fail();
    // }
    // }
    // return ranOnce;
    //
    // }
    //
    // }
    //
    // @Test
    // public void multiThreadedTest() throws Exception {
    // int threadCount = 50;
    // ExecutorService executor = Executors.newCachedThreadPool();
    // Set<Integer> indexes = new HashSet<Integer>();
    //
    // try (IdxTable idxTable = new IdxTable(file)) {
    //
    // List<B> lst = new ArrayList<B>();
    // lst.add(new R(1,idxTable, executor, 5));
    // lst.add(new R(2,idxTable, executor, 6));
    // for (int i = 0; i < threadCount; i++) {
    // lst.add(new W(i+1,idxTable, executor, 5, indexes::add));
    // }
    //
    // List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    // lst.forEach(b -> futures.add(executor.submit(b)));
    // // executor.invokeAll(lst);//, 10, TimeUnit.SECONDS);
    // Thread.sleep(3000);
    // lst.forEach(b -> b.running = false);
    // boolean hasFailures = false;
    // for (int i=0;i<futures.size();i++) {
    // Future<Boolean> f = futures.get(i);
    // try {
    // if (f.isDone()) {
    // if (f.isCancelled()) {
    // System.out.println( String.format(" %s cancelled", lst.get(i) ));
    // hasFailures=true;
    // } else {
    // if (!f.get()) {
    // System.out.println(String.format(" %s failed", lst.get(i) ));
    // hasFailures = true;
    // }
    // }
    // } else {
    // assertTrue(f.get(5, TimeUnit.SECONDS));
    // }
    //
    // } catch (InterruptedException | ExecutionException |TimeoutException e) {
    // hasFailures = true;
    // System.out.println( String.format(" %s exception: %s", lst.get(i), e ));
    // }
    // }
    // executor.shutdown();
    // lst.forEach(b -> assertTrue(b.toString() + " did not run once", b.ranOnce));
    // System.out.println( String.format( "%s threads executed on %s indexes",
    // threadCount, indexes.size() ));
    // assertTrue( "Did not test with lock collisions", indexes.size() > 40 );
    // assertFalse( "Failues listed in console", hasFailures);
    // }
    // }
}
