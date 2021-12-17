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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
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
    public Demo(Cluster.Builder builder) {
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
     * @param limit the maximum number of records to load (0=all)
     * @param threads the maximum number of threads to use.
     * @throws IOException on I/O error.
     */
    public void load(URL url, int limit, int threads) throws IOException {
        BulkExecutor bulkExecutor = new BulkExecutor(session, Executors.newFixedThreadPool(threads), threads * 50);
        try (GeoNameIterator geoNameIterator = new GeoNameIterator(url)) {
            if (limit > 0) {
                geoNameIterator.setLimit(limit);
            }
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

    public static Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "This help");
        options.addOption("c", "create", false, "create the table and index.  Will not overwrite existing");
        options.addOption("l", "load", false, "load the data");
        options.addOption("n", "load-limit", true, "Limit the number of records loaded (only valid with -l)");
        options.addOption("t", "load-threads", true, "The maximum number of threads to load data with");
        options.addOption("s", "server", true, "The server to initiate connection with. "
                + "May occure more than once.  If not specified localhost is used.");
        options.addOption("u", "user", true, "User id");
        options.addOption("p", "password", true, "Password. (Required if user is provided");

        return options;
    }

    /**
     * Main entry point.
     * <p> use {@code -h} for help
     * @param args the arguments.
     * @throws IOException on I/O error
     * @throws InterruptedException on thread interuption.
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(getOptions(), args);
        } catch (Exception e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Demo", "", getOptions(), e.getMessage());
            System.exit(1);
        }

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Demo", "", getOptions(), "");
        }

        Cluster.Builder builder = Cluster.builder();

        if (cmd.hasOption("s")) {
            for (String srv : cmd.getOptionValues("s")) {
                builder.addContactPoint(srv);
            }
        } else {
            builder.addContactPoint("localhost");
        }

        if (cmd.hasOption("u")) {
            if (cmd.hasOption("p")) {
                builder.withCredentials(cmd.getOptionValue("u"), cmd.getOptionValue('p'));
            } else {
                throw new IllegalArgumentException("Password (-p) must be provided with user.");
            }
        }

        try (Demo demo = new Demo(builder)) {

            if (cmd.hasOption("c")) {
                demo.initTable();
            }
            if (cmd.hasOption("l")) {
                int limit = 0;
                if (cmd.hasOption('n')) {
                    try {
                        limit = Integer.parseInt(cmd.getOptionValue('n'));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String
                                .format("load-limit (-n) must be a number.  '%s' provided", cmd.getOptionValue('n')));
                    }
                }
                int threads = 2;
                if (cmd.hasOption('t')) {
                    try {
                        threads = Integer.parseInt(cmd.getOptionValue('t'));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String
                                .format("load-threads (-t) must be a number.  '%s' provided", cmd.getOptionValue('t')));
                    }
                }
                demo.load(GeoNameIterator.DEFAULT_INPUT, limit, threads);
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
