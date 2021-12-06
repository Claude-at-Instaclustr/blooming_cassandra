/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.instaclustr.cassandra.bloom.idx.std;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.HasherCollection;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.instaclustr.cassandra.BulkExecutor;
import com.instaclustr.geonames.GeoName;
import com.instaclustr.geonames.GeoNameHasher;
import com.instaclustr.geonames.GeoNameIterator;
import com.instaclustr.geonames.GeoNameLoader;

/**
 * Demo of the IdxTable .
 *
 */
public class Demo implements AutoCloseable {

    /**
     * The cluster we are using.
     */
    private Cluster cluster;
    /**
     * The sesson we are using.
     */
    private Session session;

    private static final String tableName = "geoNames.geoname";
    /**
     * Create keyspace command.
     */
    private static final String keyspace = "CREATE KEYSPACE IF NOT EXISTS geoNames WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };";
    /**
     * Create table command.
     */
    private static final String table = "CREATE TABLE IF NOT EXISTS %s (geonameid text, name text, asciiname text, alternatenames text, latitude text, longitude text, feature_class text,feature_code text,country_code text,cc2 text,admin1_code text,admin2_code text, admin3_code text, admin4_code text, population text, elevation text, dem text, timezone text, modification_date text,filter blob,PRIMARY KEY (geonameid ));";

    private static final String index = "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(filter) USING 'com.instaclustr.cassandra.bloom.idx.std.BloomingIndex'"
            + " WITH OPTIONS = {'numberOfBits':'302', 'numberOfItems':'10', 'numberOfFunctions':'21' }";

    private static final String query = "SELECT * FROM %s WHERE filter = %s";

    /**
     * Constructor.
     */
    public Demo() {
        Cluster.Builder builder = Cluster.builder().addContactPoint("localhost");
        cluster = builder.build();
        session = cluster.connect();
    }

    /**
     * Close the demo.  Specifically the session and the cluster.
     */
    @Override
    public void close() {
        session.close();
        cluster.close();
    }

    /**
     * Initialize the table.  This will create the keyspace, table and index.
     */
    public void initTable() {
        session.execute(keyspace);
        session.execute(String.format(table, tableName));
        session.execute(String.format(index, tableName));
    }

    class Throttle implements Predicate<GeoName> {
        int count = 0;
        int maxBlock;
        int delay;

        Throttle(int maxBlock, int delay) {
            this.maxBlock = maxBlock;
            this.delay = delay;
        }

        @Override
        public boolean test(GeoName t) {
            count++;
            if ((count % maxBlock) == 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
            return true;
        }

    }

    /**
     * Loads Geoname data from the URL.  The URL is assumed to point to an `allCountries` formatted file.
     * See Geonames in blooming-test-helpers for info.
     * @param url the ULR to the file.
     * @throws IOException on I/O error.
     */
    public void load(URL url) throws IOException {
        BulkExecutor bulkExecutor = new BulkExecutor(session, Executors.newFixedThreadPool(2), 2);
        try (GeoNameIterator geoNameIterator = new GeoNameIterator(url)) {
            // ExtendedIterator<GeoName> iter =
            // WrappedIterator.create(geoNameIterator).filterKeep( new Throttle( 500,
            // 1000));
            GeoNameLoader.load(geoNameIterator, bulkExecutor, tableName);
        }
    }

    /**
     * Search for items matching the filter.
     * @param filter The Bloom filter to match.
     * @return the list of Matching GeoNames.
     * @throws InterruptedException
     */
    public List<GeoName> search(BloomFilter filter) throws InterruptedException {
        List<GeoName> result = new ArrayList<GeoName>();
        String statement = String.format(query, tableName, GeoName.CassandraSerde.hexString(filter));
        ResultSet resultSet = session.execute(statement);
        resultSet.forEach((row) -> result.add(GeoName.CassandraSerde.deserialize(row)));
        return result;
    }

    /**
     * Main entry point.
     * <p>If a single argument is passed (anything) the tables will be created</p>
     * @param args the arguments.
     * @throws IOException on I/O error
     * @throws InterruptedException on thread interuption.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        try (Demo demo = new Demo()) {

            System.out.println("args: " + args.length);
            if (args.length == 1) {
                demo.initTable();
                demo.load(GeoNameIterator.DEFAULT_INPUT);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                System.out.println("Enter criteria (enter to quit)");
                String s = reader.readLine();
                HasherCollection hasher = new HasherCollection();

                while (!s.isEmpty()) {
                    hasher.add(GeoNameHasher.hasherFor(s));

                    System.out.println("Enter additional criteria (enter to search)");
                    s = reader.readLine();
                    while (!s.isEmpty()) {
                        hasher.add(GeoNameHasher.hasherFor(s));
                        System.out.println("Enter additional criteria (enter to search)");
                        s = reader.readLine();
                    }

                    System.out.println("\nSearch Results:");
                    BloomFilter filter = new SimpleBloomFilter(GeoNameHasher.shape, hasher);
                    List<GeoName> results = demo.search(filter);
                    if (results.isEmpty()) {
                        System.out.println("No Results found");
                    } else {
                        results.iterator().forEachRemaining(gn -> System.out.println(String.format("%s%n%n", gn)));
                    }

                    hasher = new HasherCollection();
                    System.out.println("\nEnter criteria (enter to quit)");
                    s = reader.readLine();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
