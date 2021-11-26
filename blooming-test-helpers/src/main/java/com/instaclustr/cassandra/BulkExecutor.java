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
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.exceptions.NoMatchException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Class to perform bulk operations on the Cassandra database.
 *
 */
public class BulkExecutor {

	/*
	 * the Cassandra session to use.
	 */
	private Session session;

	private final Semaphore updatePermits;

	/*
	 * this is a map of all ResultSetFutures and the Runnables that are
	 * registered as their listener. when the future completes the runnable
	 * removes the future from the map.
	 */
	private ConcurrentHashMap<Runnable, ResultSetFuture> map = new ConcurrentHashMap<>();

	/*
	 * The service that executes the runnables.
	 */
	//private ExecutorService executor = Executors.newSingleThreadExecutor();
	private ExecutorService executor = Executors.newCachedThreadPool();
	//private ExecutorService executor = Executors.newFixedThreadPool(3);

	/**
	 * Constructor.
	 *
	 * @param session
	 *            The Cassandra session to use.
	 */
	public BulkExecutor(Session session) {
		this.session = session;
		updatePermits = new Semaphore(200);
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

	public void execute(String statement, Consumer<ResultSet> func) throws InterruptedException {

		/*
		 * runner runs when future is complete. It simply removes the future
		 * from the map to show that it is complete.
		 */
		Runnable runner = new Runnable() {
			@Override
			public void run() {
			    ResultSetFuture rsf = map.get(this);
			    ResultSet rs;
                try {
                    if (rsf != null) {
                        rs = rsf.get();
                        if (func != null) {
                            func.accept(rs);
                        }
                    }
                }
                catch (NoMatchException e) {
                    System.err.println( "No Match Detected");
                }
                catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                } finally {
                    map.remove(this);
                    updatePermits.release();
                }
			}
		};


        updatePermits.acquire();
		ResultSetFuture rsf =  session.executeAsync(statement);

		/*
		 * the map keeps a reference to the ResultSetFuture so we can track
		 * when all futures have executed
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
			 * this runnable will check the map and if it is empty notify this
			 * (calling) thread so it can continue. Otherwise it places itself
			 * back on the executor queue again.
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
					 * place the runnable on the executor thread and then wait
					 * to be notified. We will be notified when all the
					 * statements have been executed
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

}
