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
package com.instaclustr.cassandra.bloom.table;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.Hasher;
import org.apache.commons.collections4.bloomfilter.hasher.SimpleHasher;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.instaclustr.cassandra.BulkExecutor;

/**
 * A table that stores the multidimensional Bloom filter data.
 *
 */
public class IdxTable {

    /**
     * The keysspace this table is in
     */
    private final String keyspace;
    /**
     * The name of this table.
     */
    private final String tblName;
    /**
     * The session this table is using.
     */
    private Session session;

    /**
     * A mapping of bytes to CQL `IN` clauses.
     */
    private static final String[] byteTable;

    /**
     * A mapping of byte value to selectivity.
     */
    private static final int[] selectivityTable;

    /**
     * Buil the byteTable and selectivity table
     */
    static {
        // populate the byteTable
        int limit = (1 << Byte.SIZE);
        byteTable = new String[limit];
        selectivityTable = new int[limit];
        List<Integer> lst = new ArrayList<Integer>();

        for (int i = 0; i < limit; i++) {
            for (int j = 0; j < limit; j++) {
                if ((j & i) == i) {
                    lst.add(j);
                    selectivityTable[j]++;
                }
            }
            byteTable[i] = String.join(", ",
                    lst.stream().map(b -> String.format("%d", b)).collect(Collectors.toList()));
            lst.clear();
        }

    }

    /**
     * Constructor.
     * @param session The session to use.
     * @param keyspace The keyspace for this table.
     * @param tblName The name of this table.
     */
    public IdxTable(Session session, String keyspace, String tblName) {
        this.keyspace = keyspace;
        this.tblName = tblName;
        this.session = session;

    }

    /**
     * Creates the table in the keyspace.
     */
    public void create() {
        String fmt = "CREATE TABLE %s.%s ( position int, code int, tokn text, PRIMARY KEY((position), code, tokn));";
        session.execute(String.format(fmt, keyspace, tblName));
    }

    /**
     * Inserts a BitMapProducer into the table.
     * <p>Generally the token is the primary key of the base table</p>
     * @param producer The producer that generates the BitMap.
     * @param token the token to associate the resulting Bloom filter to.
     */
    public void insert(BitMapProducer producer, String token) {

        String fmt = "INSERT INTO %s.%s ( position, code, tokn ) VALUES ( %d, %d, '%s' )";
        BulkExecutor executor = new BulkExecutor(session);
        producer.forEachBitMap(new LongPredicate() {
            int pos = 0;

            @Override
            public boolean test(long word) {

                for (int i = 0; i < Long.BYTES; i++) {
                    int code = (int) (word & 0xFF);
                    word = word >> Byte.SIZE;
                    try {
                        executor.execute(String.format(fmt, keyspace, tblName, pos++, code, token));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return false;
                    }
                }
                return true;
            }
        });
        executor.awaitFinish();
    }

    /**
     * A class that contains a Set of tokens as well as a Bloom filter to quickly exclude filters we know
     * will not be in the set.  Must be thread safe.
     */
    class GatedTokens {
        /**
         * The gating bloom filter.
         */
        private BloomFilter gate = null;
        /**
         * The number of items in the set of tokens.
         */
        private int gateCount = -1;
        /**
         * The set of tokens.
         */
        private final Set<String> tokens;
        /**
         * The number of candidate tokens that were rejected byteh gate.
         */
        private int skipped;

        /**
         * Constructor.
         * <p>Constructs without a gating filter so all candidates are added.
         */
        GatedTokens() {
            tokens = new HashSet<String>();
            skipped = 0;
        }

        /**
         * Creates a new GatedTokens object with a gating Bloom filter created from all the
         * tokens in this GatedToken object.
         * @return an empty GatedTokens object with a gate Bloom filter.
         */
        GatedTokens nextSet() {
            GatedTokens result = new GatedTokens();
            if (tokens.size() > 0) {
                result.gate = getFilter();
            }
            return result;
        }

        /**
         * Returns {@code true} if the gate has no tokens.
         * @return {@code true} if the gate has no tokens.
         */
        public boolean isEmpty() {
            return gateCount > -1 && tokens.size() == 0;
        }

        /**
         * Add a token to the collection.  Will only add the token if
         * <ul>
         * <li>The token is in the gating Bloom filter; or</li>
         * <li>The gating Bloom filter is {@code null}</li>
         * @param token the token to add.
         */
        public void add(String token) {
            Hasher hasher = new SimpleHasher(token.getBytes(StandardCharsets.UTF_8));
            /*
             * if we have a gate we only want to add things that the gate says might be in
             * the set. This will reduce the overhead by removing strings that we know are
             * not going to be in the intersection of the two sets later. If we don't have a
             * gate add everything.
             */
            boolean doAdd = gate == null ? true : gate.contains(hasher);
            if (doAdd) {
                tokens.add(token);
            } else {
                skipped++;
            }
        }

        /**
         * Gets the set of tokens collected by this GatedTokens objec.t
         * @return the set of tokens.
         */
        public Set<String> getTokens() {
            return tokens;
        }

        /**
         * Rebuilds the gate based on all the tokens in the collection.
         * This method is used when:
         * <ul>
         * <li>The gating Bloom filter is requested and no gating bloom filter exits</li>
         * <li>A merge of another GatedTokens object occurs and the number of items in the resulting
         * merge is approx. an order of magnitude smaller.</li>
         * </ul>
         * <p>This can be an expensive operation for a large collection of tokens</p>
         * @param shape The shape of the resulting Bloom filter.
         */
        private void rebuildGate(Shape shape) {
            BloomFilter newfilter = new SimpleBloomFilter(shape);

            for (String s : tokens) {
                newfilter.mergeInPlace(new SimpleHasher(s.getBytes(StandardCharsets.UTF_8)));
            }
            gate = newfilter;
            gateCount = tokens.size();
        }

        /**
         * Gets the gating Bloom filter.
         * @return the gating Bloom filter for this collection of tokens.
         */
        public synchronized BloomFilter getFilter() {
            if (gate == null) {
                rebuildGate((tokens.isEmpty()) ? Shape.Factory.fromNP(1, 0.3934)
                        : Shape.Factory.fromNP(tokens.size(), 0.3934));
            }
            return gate;
        }

        /**
         * Merge a GatedTokens into this GatedTokens.
         * <p>A merge is intersection of the set of tokens in the two GatedTokens.</p>
         * <p>May rebuild the gating Bloom filter.</p>
         * @param otherTokens The other tokens to merge.
         * @throws NoMatchException If the result is an empty set of tokens.
         */
        public synchronized boolean merge(GatedTokens otherTokens)  {
            int otherCnt = otherTokens.getTokens().size();
            int otherTot = otherCnt + otherTokens.skipped;
            double otherPct = (100.0 * otherCnt) / otherTot;
            System.out.print(String.format("Merging %d of %d tokens (%.2f %%) into %d ", otherCnt, otherTot, otherPct,
                    tokens.size()));

            tokens.retainAll(otherTokens.getTokens());
            System.out.println(String.format("resulting in %d tokens.  Gate count: %s", tokens.size(), gateCount));
            if (tokens.isEmpty()) {
                return false;
            }
            if (gateCount / tokens.size() > 10) {
                rebuildGate(gate.getShape());
            }
            return true;
        }

    }

    /**
     * Captures the tokens from the index table query into a GatedTokens object.
     *
     */
    class TokenCapture implements Consumer<ResultSet> {
        /**
         * The set of extracted tokens
         */
        GatedTokens tokens = null;

        /**
         * The bulk executor making the requests to Cassandra
         */
        private final BulkExecutor executor;

        /**
         * Constructor.
         * @param executor The bulk executor being used.
         */
        TokenCapture(BulkExecutor executor) {
            this.executor = executor;
        }

        @Override
        public void accept(ResultSet rs) {
            GatedTokens results = tokens == null ? new GatedTokens() : tokens.nextSet();
            rs.forEach(row -> results.add(row.getString(0)));
            setTokens(results);
        }

        /**
         * Merges the results into the current set.
         * <p>If there are not yet any tokens set the tokens rather than merges.</p>
         * <p><em>If the result is an empty set, this method kills the executor to stop the process</em></p>
         * @param results the gatedTokens to merge.
         */
        private synchronized void setTokens(GatedTokens results) {
            if (tokens == null) {
                tokens = results;
            } else {
                if (!tokens.merge(results)) {
                    executor.kill();
                }
            }
        }
    }

    /**
     * Searches the index.
     * @param producer The BitMapProducer to search with.
     * @return The matching set of tokens.
     */
    public Set<String> search(BitMapProducer producer) {
        BulkExecutor executor = new BulkExecutor(session);
        String fmt = "SELECT tokn FROM %s.%s WHERE position=%d AND code in (%s)";

        // create a set of high selectivity to low selectivity codes
        TokenCapture capture = new TokenCapture(executor);
        Map<Integer, List<Pair<Integer, Integer>>> order = new TreeMap<Integer, List<Pair<Integer, Integer>>>(
                Collections.reverseOrder());
        try {
            producer.forEachBitMap(new LongPredicate() {
                int pos = 0;

                @Override
                public boolean test(long word) {
                    for (int i = 0; i < Long.BYTES; i++) {
                        int code = (int) (word & 0xFF);
                        word = word >> Byte.SIZE;
                        if (code > 0) {
                            List<Pair<Integer, Integer>> lst = order.get(selectivityTable[code]);
                            if (lst == null) {
                                lst = new ArrayList<Pair<Integer, Integer>>();
                                order.put(selectivityTable[code], lst);
                            }
                            lst.add(new ImmutablePair<Integer, Integer>(pos, code));
                        }
                        pos++;
                    }
                    return true;
                }
            });
            // execute the high selectivity first.
            for (Map.Entry<Integer, List<Pair<Integer, Integer>>> entry : order.entrySet()) {
                if (entry.getKey() > 0 && !entry.getValue().isEmpty()) {
                    entry.getValue().forEach(pair -> {
                        try {
                            if (pair.getRight() != 0) {
                                System.out.println(String.format("Processing 0x%02X at %d [%s]", pair.getRight(),
                                        pair.getLeft(), byteTable[pair.getRight()]));
                                executor.execute(String.format(fmt, keyspace, tblName, pair.getLeft(),
                                        byteTable[pair.getRight()]), capture);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                }
            }

            executor.awaitFinish();
            if (capture.tokens == null) {
                System.err.println("All records selected");
                return Collections.emptySet();
            }
            return capture.tokens.getTokens();
        } finally {
            executor.kill();
        }
    }

}
