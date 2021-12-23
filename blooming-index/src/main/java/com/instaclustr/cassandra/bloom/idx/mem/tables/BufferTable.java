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
package com.instaclustr.cassandra.bloom.idx.mem.tables;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Stack;
import java.util.function.IntConsumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.bloom.idx.mem.tables.BufferTableIdx.IdxEntry;
import com.instaclustr.cassandra.bloom.idx.mem.tables.IdxMap.MapEntry;

public class BufferTable extends BaseTable {

    private static final Logger LOG = LoggerFactory.getLogger(BufferTable.class);

    public static final int UNSET = -1;
    final IdxMap idxTable;
    final BufferTableIdx keyTableIdx;

    @Override
    public void drop() {
        idxTable.drop();
        keyTableIdx.drop();
        super.drop();
    }

    @Override
    public void close() throws IOException {
        closeQuietly(idxTable);
        closeQuietly(keyTableIdx);
        super.close();
    }

    public static Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "This help");
        Option option = new Option("b", "block-size", true, "The size in bytes of the table blocks");
        option.setRequired(true);
        options.addOption(option);
        option = new Option("i", "input", true, "The Buffer Table file to process.");
        option.setRequired(true);
        options.addOption(option);
        options.addOption("o", "output", true, "Output file.  If not specified results will not be preserved");
        return options;
    }

    public static void main(String[] args) throws IOException {
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(getOptions(), args);
        } catch (Exception e) {
            formatter.printHelp("BufferTable", "", getOptions(), e.getMessage());
            System.exit(1);
        }

        if (cmd.hasOption("h")) {
            formatter.printHelp("BufferTable", "", getOptions(), "");
            System.exit(0);
        }

        File in = new File(cmd.getOptionValue("i"));
        if (!in.exists()) {
            formatter.printHelp("BufferTable", String.format("%s does not exist", in.getAbsoluteFile()), getOptions(),
                    "");
            System.exit(1);
        }
        int blockSize = 0;
        try {
            blockSize = Integer.parseInt(cmd.getOptionValue("b"));
        } catch (NumberFormatException e) {
            formatter.printHelp("BufferTable",
                    String.format("%s can not be parsed as an integer", cmd.getOptionValue("b")), getOptions(),
                    e.getMessage());
            System.exit(1);
        }

        PrintStream out = System.out;
        if (cmd.hasOption("o")) {
            File f = new File(cmd.getOptionValue("o"));
            if (!f.getParentFile().exists()) {
                formatter.printHelp("BufferTable", String.format("Directory %s must exist", cmd.getOptionValue("o")),
                        getOptions(), "");
                System.exit(1);
            }
            out = new PrintStream(f);
        }

        out.println(
                "'index','initialized','reference','idx_init','available','deleted','invalid','used','allocated','offset','hex','char'");

        try (BufferTable bufferTable = new BufferTable(in, blockSize, BaseTable.READ_ONLY)) {

            int blocks = (int) bufferTable.idxTable.getFileSize() / bufferTable.idxTable.getBlockSize();
            for (int block = 0; block < blocks; block++) {
                MapEntry mapEntry = bufferTable.idxTable.get(block);
                if (mapEntry.isInitialized()) {
                    IdxEntry idxEntry = bufferTable.keyTableIdx.get(mapEntry.getKeyIdx());

                    out.print(String.format("%s,'true',%s,%s,%s,%s,%s,%s,%s,%s,", block, mapEntry.getKeyIdx(),
                            idxEntry.isInitialized(), idxEntry.isAvailable(), idxEntry.isDeleted(),
                            idxEntry.isInvalid(), idxEntry.getLen(), idxEntry.getAlloc(), idxEntry.getOffset()));
                    ByteBuffer value = null;
                    if (!idxEntry.isInvalid()) {
                        value = bufferTable.get(block);
                    }

                    if (value == null) {
                        out.println(",");
                    } else {
                        out.print("'0x");
                        for (int i = 0; i < idxEntry.getLen(); i++) {
                            out.print(String.format("%2x", value.get(value.position() + i)));
                        }
                        out.print("','");
                        for (int i = 0; i < idxEntry.getLen(); i++) {
                            out.print(String.format("%s", (char) value.get(value.position() + i)));
                        }
                        out.println("'");
                    }
                } else {
                    out.println(String.format("%s,'false',%s,,,,,,,,", block, mapEntry.getKeyIdx()));
                }
            }
        }
    }

    public BufferTable(File file, int blockSize) throws IOException {
        this(file, blockSize, BaseTable.READ_WRITE);
    }

    public BufferTable(File file, int blockSize, boolean readOnly) throws IOException {
        super(file, blockSize, readOnly);
        File idxFile = new File(file.getParentFile(), file.getName() + "_idx");
        idxTable = new IdxMap(idxFile, readOnly);
        File keyIdxFile = new File(file.getParentFile(), file.getName() + "_keyidx");
        keyTableIdx = new BufferTableIdx(keyIdxFile, readOnly);
    }

    public ByteBuffer get(int idx) throws IOException {
        if (!idxTable.hasBlock(idx)) {
            LOG.warn("Attempted to retrieve unknown record {}.   Block not in index table.", idx);
            return null;
        }
        IdxMap.MapEntry mapEntry = idxTable.get(idx);
        if (!mapEntry.isInitialized()) {
            LOG.warn("Attempted to retrieve unknown record {}.   Map to {} not initialized.", idx,
                    mapEntry.getKeyIdx());
            return null;
        }
        BufferTableIdx.IdxEntry idxEntry = keyTableIdx.get(mapEntry.getKeyIdx());
        if (idxEntry.isDeleted()) {
            LOG.info("Attempted to retrieve deleted record {}.", idx);
            return null;
        }
        ByteBuffer buff = getBuffer();
        int pos = idxEntry.getOffset();
        buff.position(pos);
        buff.limit(pos + idxEntry.getLen());
        return buff;
    }

    /**
     * Creates Adds a block to the buffer table.
     * The returned IdxEntry is not initializes, so it is not visible in searches
     * calling method should set the initialized state when ready.
     * @param buff the buffer to add to the table.
     * @return the IdxEntry for the buffer.
     * @throws IOException
     */
    private BufferTableIdx.IdxEntry add(ByteBuffer buff) throws IOException {
        int length = buff.remaining();
        int position = extendBytes(length);
        ByteBuffer writeBuffer = getWritableBuffer();
        writeBuffer.position(position);
        sync(() -> writeBuffer.put(buff), position, length, 4);
        return keyTableIdx.addBlock(position, length);
    }

    /**
     * Set an existing index to the buffer data.
     * @param keyIdxEntry
     * @param buff
     * @throws IOException
     */
    private void set(BufferTableIdx.IdxEntry keyIdxEntry, ByteBuffer buff) throws IOException {
        //Stack<Func> undo = new Stack<Func>();
        try (RangeLock lock = keyIdxEntry.lock()) {
            keyIdxEntry.setLen(buff.remaining());
            ByteBuffer writeBuffer = getWritableBuffer();
            writeBuffer.position(keyIdxEntry.getOffset());
            sync(() -> writeBuffer.put(buff), keyIdxEntry.getOffset(), keyIdxEntry.getLen(), 4);
            keyIdxEntry.setDeleted(false);
        } catch (IOException e) {
            execQuietly(() -> keyIdxEntry.setDeleted(true));
            throw e;
        }
    }

    private BufferTableIdx.IdxEntry createNewEntry(int idx, ByteBuffer buff) throws IOException {

        BufferTableIdx.IdxEntry keyIdxEntry = null;
        try {
            keyIdxEntry = keyTableIdx.search(buff.remaining());
        } catch (IOException e1) {
            // ignore and try to create a new one.
        }
        try {
            if (keyIdxEntry == null) {
                keyIdxEntry = add(buff);
                idxTable.get(idx).setKeyIdx(keyIdxEntry.getBlock());
                keyIdxEntry.setInitialized(true);
            } else {
                set(keyIdxEntry, buff);
            }
            return keyIdxEntry;
        } catch (IOException e) {
            if (keyIdxEntry != null) {
                try {
                    BufferTableIdx.IdxEntry entry = keyIdxEntry;
                    keyTableIdx.retryOnTimeout( () -> {
                        entry.setInitialized(true);
                        entry.setDeleted(true);
                    });
                } catch (Exception e1) {
                    LOG.error( "Error freeing keyIdx "+keyIdxEntry.getOffset(), e );
                }
            }
            throw e;
        }

    }

    public void set(int idx, ByteBuffer buff) throws IOException {
        IdxMap.MapEntry mapEntry = idxTable.get(idx);
        if (mapEntry.isInitialized()) {
            // we are reusing the key so see if the buffer fits.
            BufferTableIdx.IdxEntry keyIdxEntry = keyTableIdx.get(mapEntry.getKeyIdx());
            LOG.debug( "checking for {} bytes. {}", buff.remaining(), keyIdxEntry.getAlloc()  );
            if (keyIdxEntry.getAlloc() >= buff.remaining()) {
                set(keyIdxEntry, buff);
            } else {
                // buffer did not fit so we need a new one and we need to free the old one.
                BufferTableIdx.IdxEntry newKeyIdxEntry = null;
                while (newKeyIdxEntry == null) {
                    try {
                        newKeyIdxEntry = createNewEntry(idx, buff);
                    } catch (IOException e) {
                        LOG.info(String.format("Problems creating new entry for %s", idx), e);
                    }
                }
                mapEntry.setKeyIdx(newKeyIdxEntry.getBlock());
                keyIdxEntry.setDeleted(true);
            }
        } else {
            // this is a new key so just write it.
            BufferTableIdx.IdxEntry keyIdxEntry = createNewEntry(idx, buff);
            mapEntry.setKeyIdx(keyIdxEntry.getBlock());
        }
    }

    public void delete(int idx) throws IOException {
        IdxMap.MapEntry mapEntry = idxTable.get(idx);
        if (mapEntry.isInitialized()) {
            BufferTableIdx.IdxEntry keyIdx = keyTableIdx.get(mapEntry.getKeyIdx());
            try (RangeLock keyLock = keyIdx.lock(Integer.MAX_VALUE)) {
                keyIdx.setDeleted(true);
            }
        }
    }

    public void search(IntConsumer consumer, IdxMap.SearchEntry target) {
        idxTable.search(consumer, target);
    }
}
