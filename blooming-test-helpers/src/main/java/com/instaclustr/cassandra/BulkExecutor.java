/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.instaclustr.cassandra;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.QueryConsistencyException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

/**
 * Class to perform bulk operations on the Cassandra database.
 *
 */
public class BulkExecutor {

    private static final Logger logger = LoggerFactory.getLogger(BulkExecutor.class);


    /*
     * the Cassandra session to use.
     */
    private final Session session;

    private final Semaphore updatePermits;

    /*
     * this is a map of all ResultSetFutures and the Runnables that are registered
     * as their listener. when the future completes the runnable removes the future
     * from the map.
     */
    private ConcurrentHashMap<Runnable, ResultSetFuture> map = new ConcurrentHashMap<>();

    /*
     * The service that executes the runnables.
     */
    // private ExecutorService executor = Executors.newSingleThreadExecutor();
    //private ExecutorService executor = Executors.newCachedThreadPool();
    //private ExecutorService executor = Executors.newFixedThreadPool(3);
    private final ExecutorService executor;
    /**
     * Constructor.
     *
     * @param session
     *            The Cassandra session to use.
     */
    public BulkExecutor(Session session) {
        this( session, Executors.newFixedThreadPool(session.getCluster().getMetrics().getKnownHosts().getValue()) );
    }

    public BulkExecutor(Session session, ExecutorService executor) {
        this( session, executor, session.getCluster().getMetrics().getKnownHosts().getValue());
    }


    public BulkExecutor(Session session, ExecutorService executor, int semaphoreCount) {
        this.session = session;
        this.updatePermits = new Semaphore(semaphoreCount);
        this.executor = executor;
    }

    public void kill() {
        for (ResultSetFuture rsf : map.values()) {
            rsf.cancel(true);
        }
        map.clear();
    }

    /**
     * Execute a number of statements.
     *
     * All output from the statements are discarded.
     *
     * Statement order is not guaranteed.
     *
     * May be called multiple times. May not be called after awaitFinish().
     *
     * @see awaitFinish
     *
     * @param statements
     *            An iterator of statements to execute.
     * @throws InterruptedException
     */
    public void execute(String statement) throws InterruptedException {
        execute(statement, null);
    }

    public void execute(String statement, Consumer<ResultSet> consumer) throws InterruptedException {

        /*
         * runner runs when future is complete. It simply removes the future from the
         * map to show that it is complete.
         */
        Runnable runner = new ExecRunner( statement, consumer );

        updatePermits.acquire();
        ResultSetFuture rsf = session.executeAsync(statement);
        /*
         * the map keeps a reference to the ResultSetFuture so we can track when all
         * futures have executed
         */
        map.put(runner, rsf);
        // the ResultSetFuture will execute the runner on the executor.
        rsf.addListener(runner, executor);

    }

    /**
     * Wait for the executor to complete all executions.
     */
    public void awaitFinish() {
        /* if the map is not empty we need to wait until it is */
        if (!map.isEmpty()) {
            /*
             * this runnable will check the map and if it is empty notify this (calling)
             * thread so it can continue. Otherwise it places itself back on the executor
             * queue again.
             */
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    synchronized (this) {
                        if (map.isEmpty()) {
                            notify();
                        } else {
                            executor.execute(this);
                        }
                    }
                }
            };
            synchronized (r) {
                try {
                    /*
                     * place the runnable on the executor thread and then wait to be notified. We
                     * will be notified when all the statements have been executed
                     */
                    executor.execute(r);
                    r.wait();
                } catch (InterruptedException e) {
                    System.err.println("Interrupted waiting for map to clear");
                    e.printStackTrace();
                }
            }
        }
        executor.shutdown();
    }

    class ExecRunner implements Runnable {
        private final String stmt;
        private final Consumer<ResultSet> func;

        ExecRunner( String statement, Consumer<ResultSet> consumer) {
            stmt = statement;
            func = consumer;
        }

        private void processResultSet( ResultSet rs) {
            if (func != null) {
                func.accept(rs);
            }
        }
        @Override
        public void run() {
            ResultSetFuture rsf = map.get(this);
            try {
                if (rsf != null) {
                    processResultSet( rsf.get() );
                }
            } catch (Exception e) {
                if (e.getCause() != null) {
                    if (e.getCause() instanceof QueryConsistencyException ||
                            e.getCause() instanceof ConnectionException) {
                        logger.warn( "Write failure on {}", stmt);
                        try {
                            processResultSet( session.execute(stmt) );
                        } catch (Exception e1) {
                            logger.error( "Exec runner failure", e );
                        }
                    } else {
                        logger.error( "Exec runner failure", e );
                    }
                } else {
                    logger.error( "Exec runner failure", e );
                }

            } finally {
                map.remove(this);
                updatePermits.release();
            }
        }

    };

}
