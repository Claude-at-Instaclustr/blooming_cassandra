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
package com.instaclustr.cassandra.bloom.idx.mem;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.concurrent.Future;
import java.util.function.IntConsumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.mem.tables.BloomTable;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.Func;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable.OutputTimeoutException;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BaseTable;
import com.instaclustr.cassandra.bloom.idx.mem.tables.BitTable;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndexer;

/**
 * This is what Daniel Lemire called Bloofi2. Basically, instead of using a tree
 * structure like Bloofi, we "transpose" the BitSets.
 */
public final class FlatBloofi implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BloomingIndexer.class);

    /*
     * each buffer entry accounts for 64 entries in the index. each there is one
     * long in each buffer entry for each bit in the bloom filter. each long is a
     * bit packed set of 64 flags, one for each entry.
     */
    private final BitTable busy;
    /*
     * The Bloom table
     */
    private final BloomTable buffer;
    /**
     * The number of bits in each Bloom filter.
     */
    private final int numberOfBits;

    /**
     * Creates the options for the command line utility.
     * @return the Options structure.
     */
    public static Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "This help");
        Option option = new Option("n", "bits", true, "The number of bits in the Bloom filters");
        option.setRequired(true);
        options.addOption(option);
        option = new Option("d", "directory", true, "The directory with FlatBloofi files to process.");
        option.setRequired(true);
        options.addOption(option);
        options.addOption("o", "output", true, "Output file.  If not specified results will be printed to Standard out.");
        options.addOption("i", "index", true, "A specific index to display/dump.  May be specified more than once.");
        return options;
    }

    /**
     * Tehe command line utility.
     * @param args The arguments for the options.
     * @throws IOException on IO Error.
     */
    public static void main(String[] args) throws IOException {
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(getOptions(), args);
        } catch (Exception e) {
            formatter.printHelp("FlatBloofi", "", getOptions(), e.getMessage());
            System.exit(1);
        }

        if (cmd.hasOption("h")) {
            formatter.printHelp("FlatBloofi", "", getOptions(), "");
            System.exit(0);
        }

        File in = new File(cmd.getOptionValue("d"));
        if (!in.exists()) {
            formatter.printHelp("FlatBloofi", String.format("%s does not exist", in.getAbsoluteFile()), getOptions(),
                    "");
            System.exit(1);
        }
        int numberOfBits = 0;
        try {
            numberOfBits = Integer.parseInt(cmd.getOptionValue("n"));
        } catch (NumberFormatException e) {
            formatter.printHelp("FlatBloofi",
                    String.format("%s can not be parsed as an integer", cmd.getOptionValue("b")), getOptions(),
                    e.getMessage());
            System.exit(1);
        }

        PrintStream out = System.out;
        if (cmd.hasOption("o")) {
            File f = new File(cmd.getOptionValue("o"));
            if (!f.getParentFile().exists()) {
                formatter.printHelp("FlatBloofi", String.format("Directory %s must exist", cmd.getOptionValue("o")),
                        getOptions(), "");
                System.exit(1);
            }
            out = new PrintStream(f);
        }

        out.println("'index','deleted','filter'");
        try (FlatBloofi flatBloofi = new FlatBloofi(in, numberOfBits, BaseTable.READ_ONLY)) {

            if (cmd.hasOption("i")) {
                for (String idxStr : cmd.getOptionValues("i")) {
                    try {
                        int idx = Integer.parseInt(idxStr);
                        printEntry(flatBloofi.getEntry(idx), numberOfBits, out);
                    } catch (NumberFormatException e) {
                        System.err.format("%s can not be parsed as a number%n", idxStr);
                    }
                }
            } else {
                for (int idx = 0; idx < flatBloofi.getMaxIndex(); idx++) {
                    printEntry(flatBloofi.getEntry(idx), numberOfBits, out);
                }
            }
        }
    }

    /**
     * Prints a CSV entry for the utility code.
     * @param entry the entry to print.
     * @param numberOfBits the number of bits in the filter.
     * @param out the PrintStream to write to.
     */
    private static void printEntry(Entry entry, int numberOfBits, PrintStream out) {
        BitMapProducer producer = BitMapProducer.fromIndexProducer(entry.getFilter(), numberOfBits);

        out.format("%s,%s,'0x", entry.getIndex(), entry.isDeleted());
        producer.forEachBitMap((w) -> {
            out.format("%016x", w);
            return true;
        });
        out.println("'");

    }

    /**
     * Constructor. Opens FlatBloofi in Read/Write mode.
     * @param dir the directory where the FlatBloofi files should be stored.
     * @param numberOfBits the number of bits in the Bloom filters.
     * @throws IOException on IO error.
     */
    public FlatBloofi(File dir, int numberOfBits) throws IOException {
        this(dir, numberOfBits, BaseTable.READ_WRITE);
    }

    /**
     * Constructor.
     * @param dir the directory where the FlatBloofi files should be stored.
     * @param numberOfBits the number of bits in the Bloom filters.
     * @param readOnly If {@code true}, FlatBloofi is opened in Read only mode.
     * @throws IOException on IO error.
     */
    public FlatBloofi(File dir, int numberOfBits, boolean readOnly) throws IOException {
        this.numberOfBits = numberOfBits;
        buffer = new BloomTable(numberOfBits, new File(dir, "BloomTable"), readOnly);
        try {
            busy = new BitTable(new File(dir, "BusyTable"), readOnly);
        } catch (IOException e) {
            BaseTable.closeQuietly(buffer);
            throw e;
        }
    }

    /**
     * Executes a function using the Busy table executor.
     * @param fn the Func to execute.
     * @return
     * @see BaseTable.Func
     */
    public Future<?> exec(Func fn) {
        return busy.exec(fn);
    }

    @Override
    public void close() throws IOException {
        try {
            busy.close();
        } catch (IOException e) {
            BaseTable.closeQuietly(buffer);
            throw e;
        }
        buffer.close();
    }

    /**
     * Adds the bloom filter to the FlatBloofi.
     * @param bloomFilter the Bloom filter to add.
     * @return the Bloom filter index
     * @throws IOException on IO Error
     */
    public int add(ByteBuffer bloomFilter) throws IOException {

        int idx = -1;
        while (idx < 0) {
            try {
                idx = busy.newIndex();
            } catch (OutputTimeoutException e) {
                logger.debug("Timeout trying to get new idx, trying again");
            } catch (IOException e) {
                logger.error("Error {} attempting to get new idx", e.getMessage());
                throw e;
            }
        }
        while (true) {
            try {
                buffer.setBloomAt(idx, bloomFilter.asLongBuffer());
                return idx;
            } catch (OutputTimeoutException e) {
                logger.debug("Timeout writing Bloom filter {}, trying again", idx);
            } catch (IOException e) {
                final int idxToClear = idx;
                busy.requeue(() -> busy.clear(idxToClear));
                logger.warn("Error {} attempting to write Bloom filter {}", e, idx);
                throw e;
            }
        }

    }

    /**
     * Updates a Bloom filter.
     * @param idx the Bloom filter index.
     * @param bloomFilter The new Bloom filter value.
     * @throws IOException on IO Error.
     */
    public void update(int idx, ByteBuffer bloomFilter) throws IOException {
        buffer.setBloomAt(idx, bloomFilter.asLongBuffer());
    }

    /**
     * Adjusts a Bloom filter buffer to ensure that it is long enough to properly fit into a LongBuffer.
     * If the filter does not end on a Long boundary zero values bytes are added as appropriate.
     * @param bloomFilter the Bloom filter to adjust.
     * @return A properly sized Bloom filter
     * @throws IllegalArgumentException if the filter is too long.
     */
    private LongBuffer adjustBuffer(ByteBuffer bloomFilter) {
        if (bloomFilter.remaining() == BitMap.numberOfBitMaps(numberOfBits) * Long.BYTES) {
            return bloomFilter.asLongBuffer().asReadOnlyBuffer();
        }
        if (bloomFilter.remaining() > BitMap.numberOfBitMaps(numberOfBits) * Long.BYTES) {
            throw new IllegalArgumentException("Bloom filter is too long");
        }
        // must be shorter

        byte[] buff = new byte[BitMap.numberOfBitMaps(numberOfBits) * Long.BYTES];

        bloomFilter.get(buff, bloomFilter.position(), bloomFilter.remaining());
        return ByteBuffer.wrap(buff).asLongBuffer().asReadOnlyBuffer();
    }

    /**
     * Searches for matching Bloom filters.
     * @param result An IntConsumer that accepts the Bloom filter indexes.
     * @param bloomFilter The Bloom filter to search for.
     * @throws IOException on IO Error.
     */
    public void search(IntConsumer result, ByteBuffer bloomFilter) throws IOException {
        buffer.search(result, adjustBuffer(bloomFilter), busy);
    }

    /**
     * Deletes a bloom Bloom filter.
     * @param bloomFilter The Bloom filter to delete.
     * @throws IOException on IO Error.
     */
    public void delete(int idx) throws IOException {
        busy.clear(idx);
    }

    /**
     * Counts the number of active Bloom filters.
     * @return the number of active Bloom filters.
     * @throws IOException on IO Error.
     */
    public int count() throws IOException {
        return busy.cardinality();
    }

    /**
     * Gets the highest active index.
     * @return the highest used index.
     * @throws IOException on IO Error.
     */
    public int getMaxIndex() throws IOException {
        return busy.getMaxIndex();
    }


    /**
     * Gets the Entry associated with an index.
     * @return the highest used index.
     * @throws IOException on IO Error
     * @see Entry
     */
    public Entry getEntry(int idx) {
        return new Entry(idx);
    }

    /**
     * Drops the FlatBloofi index.
     * All data are deleted.
     */
    public void drop() {
        buffer.drop();
        busy.drop();
    }

    /**
     * An entry in a FlatBloofi.
     */
    public class Entry {
        /**
         * The index for the entry.
         */
        private int idx;

        /**
         * Constructor.
         * @param idx the index for the entry.
         */
        private Entry(int idx) {
            this.idx = idx;
        }

        /**
         * Gets the index for this entry.
         * @return the index for the entry
         */
        public int getIndex() {
            return idx;
        }

        /**
         * Determine if the entry is deleted.
         * @return {@code true} if the entry is deleted, {@code false} otherwise.
         */
        public boolean isDeleted() {
            try {
                return busy.retryOnTimeout(() -> {
                    return busy.isSet(idx);
                });
            } catch (Exception e) {
                logger.error("Error getting deleted status for " + idx, e);
                return true;
            }
        }

        /**
         * Gets the filter associated with the entry.
         * @return The IndexProducer for the Bloom filter data.
         */
        public IndexProducer getFilter() {
            return buffer.getBloomAt(idx);
        }

    }
}
