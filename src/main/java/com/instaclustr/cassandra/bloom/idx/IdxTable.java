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
package com.instaclustr.cassandra.bloom.idx;

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
import java.util.stream.Collectors;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.exceptions.NoMatchException;
import org.apache.commons.collections4.bloomfilter.hasher.Hasher;
import org.apache.commons.collections4.bloomfilter.hasher.SimpleHasher;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.instaclustr.cassandra.bloom.BulkExecutor;

public class IdxTable {

    private final String keyspace;
    private final String tblName;
    private Session session;

    /**
     * A list of bytes to matching bytes in the bloom filter.
     */
    private static final String[] byteTable;
    private static final int[] selectivityTable;

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

    public IdxTable(Session session, String keyspace, String tblName) {
        this.keyspace = keyspace;
        this.tblName = tblName;
        this.session = session;

    }

    public void create() {
        String fmt = "CREATE TABLE %s.%s ( position int, code int, tokn text, PRIMARY KEY((position), code, tokn));";
        session.execute(String.format(fmt, keyspace, tblName));
    }

    public void insert(BitMapProducer producer, String token) {

        String fmt = "INSERT INTO %s.%s ( position, code, tokn ) VALUES ( %d, %d, '%s' )";
        BulkExecutor executor = new BulkExecutor(session);
        producer.forEachBitMap(new LongConsumer() {
            int pos = 0;

            @Override
            public void accept(long word) {

                for (int i = 0; i < Long.BYTES; i++) {
                    int code = (int) (word & 0xFF);
                    word = word >> Byte.SIZE;
                    try {
                        executor.execute(String.format(fmt, keyspace, tblName, pos++, code, token));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        executor.awaitFinish();
    }

    class GatedTokens {
        private BloomFilter gate = null;
        private int gateCount = -1;
        private final Set<String> tokens;
        private int skipped;

        GatedTokens() {
            tokens = new HashSet<String>();
            skipped = 0;
        }

        GatedTokens nextSet() {
            GatedTokens result = new GatedTokens();
            if (tokens.size() > 0) {
                result.gate = getFilter();
            }
            return result;
        }

        public boolean isEmpty() {
            return gateCount > -1 && tokens.size() == 0;
        }

        public void add(String s) {
            Hasher hasher = new SimpleHasher(s.getBytes(StandardCharsets.UTF_8));
            /*
             * if we have a gate we only want to add things that the gate says might be in
             * the set. This will reduce the overhead by removing strings that we know are
             * not going to be in the intersection of the two sets later. If we don't have a
             * gate add everything.
             */
            boolean doAdd = gate == null ? true : gate.contains(hasher);
            if (doAdd) {
                tokens.add(s);
            } else {
                skipped++;
            }
        }

        public Set<String> getTokens() {
            return tokens;
        }

        private void rebuildGate(Shape shape) {
            BloomFilter newfilter = new SimpleBloomFilter(shape);

            for (String s : tokens) {
                newfilter.mergeInPlace(new SimpleHasher(s.getBytes(StandardCharsets.UTF_8)));
            }
            gate = newfilter;
            gateCount = tokens.size();
        }

        public synchronized BloomFilter getFilter() {
            if (gate == null) {
                rebuildGate((tokens.isEmpty()) ? Shape.Factory.fromNP(1, 0.3934)
                        : Shape.Factory.fromNP(tokens.size(), 0.3934));
            }
            return gate;
        }

        public synchronized void merge(GatedTokens otherTokens) throws NoMatchException {
            int otherCnt = otherTokens.getTokens().size();
            int otherTot = otherCnt + otherTokens.skipped;
            double otherPct = (100.0 * otherCnt) / otherTot;
            System.out.print(String.format("Merging %d of %d tokens (%.2f %%) into %d ", otherCnt, otherTot, otherPct,
                    tokens.size()));

            tokens.retainAll(otherTokens.getTokens());
            System.out.println(String.format("resulting in %d tokens.  Gate count: %s", tokens.size(), gateCount));
            if (tokens.isEmpty()) {
                throw new NoMatchException();
            }
            if (gateCount / tokens.size() > 10) {
                rebuildGate(gate.getShape());
            }
        }

    }

    class TokenCapture implements Consumer<ResultSet> {
        GatedTokens tokens = null;

        private final BulkExecutor executor;

        TokenCapture(BulkExecutor executor) {
            this.executor = executor;
        }

        @Override
        public void accept(ResultSet rs) {
            GatedTokens results = tokens == null ? new GatedTokens() : tokens.nextSet();
            rs.forEach(row -> results.add(row.getString(0)));
            setTokens(results);
        }

        private synchronized void setTokens(GatedTokens results) {
            if (tokens == null) {
                tokens = results;
            } else {
                try {
                    tokens.merge(results);
                } catch (NoMatchException e) {
                    executor.kill();
                }
            }
        }
    }

    public Set<String> search(BitMapProducer producer) {
        BulkExecutor executor = new BulkExecutor(session);
        String fmt = "SELECT tokn FROM %s.%s WHERE position=%d AND code in (%s)";

        // create a set of high selectivity to low selectivity codes
        TokenCapture capture = new TokenCapture(executor);
        Map<Integer, List<Pair<Integer, Integer>>> order = new TreeMap<Integer, List<Pair<Integer, Integer>>>(
                Collections.reverseOrder());
        try {
            producer.forEachBitMap(new LongConsumer() {
                int pos = 0;

                @Override
                public void accept(long word) {
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
                }
            });
            // execute the high selectivity first.
            for (Map.Entry<Integer, List<Pair<Integer, Integer>>> entry : order.entrySet()) {
                if (entry.getKey() > 0 && !entry.getValue().isEmpty()) {
                    entry.getValue().forEach(pair -> {
                        try {
                            if (pair.getRight() != 0) {
                                System.out.println(String.format("Processing %02X at %d [%s]", pair.getRight(),
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
                throw new NoMatchException();
            }
            return capture.tokens.getTokens();
        } catch (NoMatchException e) {
            return Collections.emptySet();
        } finally {
            executor.kill();
        }
    }

}
