package com.instaclustr.cassandra.bloom.idx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.exceptions.NoMatchException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.instaclustr.cassandra.bloom.BulkExecutor;

public class IdxTable {

    private final String keyspace;
    private final String tblName;
    private Session session;
    private BulkExecutor executor;

    /**
     * A list of bytes to matching bytes in the bloom filter.
     */
    private static final String[] byteTable;


    static {
        // populate the byteTable
        int limit = (1<<Byte.SIZE);
        byteTable = new String[limit];
        List<Integer> lst = new ArrayList<Integer>();

        for (int i = 0; i < limit; i++) {
            for (int j = 0; j < limit; j++) {
                if ((j & i) == i) {
                    lst.add(j);
                }
            }
            byteTable[i] = String.join( ", ", lst.stream().map( b -> String.format("%d", b)).collect( Collectors.toList()));
            lst.clear();
        }

    }

    public IdxTable(Session session, String keyspace, String tblName) {
        this.keyspace = keyspace;
        this.tblName = tblName;
        this.session = session;
        this.executor = new BulkExecutor( session );
    }

    public void create() {
        String fmt = "CREATE TABLE %s.%s ( position int, code int, tokn text, PRIMARY KEY((position), code, tokn));";
        session.execute( String.format( fmt,  keyspace, tblName ));
    }

    public void insert( BitMapProducer producer, String token) {

        String fmt = "INSERT INTO %s.%s ( position, code, tokn ) VALUES ( %d, %d, '%s' )";

        producer.forEachBitMap( new LongConsumer() {
            int pos=0;
            @Override
            public void accept(long word) {

                for (int i = 0; i<Long.BYTES;i++)
                {
                    int code = (int) (word & 0xFF);
                    word = word >> Byte.SIZE;
                    try {
                        executor.execute( String.format( fmt, keyspace, tblName, pos++, code, token ));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }} );

    }

    class TokenCapture implements Consumer<ResultSet> {
        Set<String> tokens = null;
        @Override
        public void accept(ResultSet rs) {
            Set<String> results = new HashSet<String>();
            rs.forEach( row -> results.add( row.getString(0)) );
            setTokens( results );
        }

        private synchronized void setTokens( Set<String> results ) {
            if (tokens == null) {
                tokens = results;
            } else {
                tokens.retainAll(results);
                if (tokens.isEmpty()) {
                    throw new NoMatchException();
                }
            }
        }
    }

    public Set<String> search( BitMapProducer producer ) {
        BulkExecutor executor = new BulkExecutor( session );
        String fmt = "SELECT tokn FROM %s.bloom_idx WHERE position=%d AND code in (%s)";

        TokenCapture capture = new TokenCapture();
        try {
        producer.forEachBitMap( new LongConsumer() {
            int pos=0;
            @Override
            public void accept(long word) {
                for (int i = 0; i<Long.BYTES;i++)
                {
                    int code = (int) (word & 0xFF);
                    word = word >> Byte.SIZE;
                    if (code > 0) {
                        try {
                            executor.execute( String.format( fmt, keyspace, pos++, byteTable[code]), capture );
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }} );
        executor.awaitFinish();
        return capture.tokens;
        } catch (NoMatchException e) {
            return Collections.emptySet();
        }
    }

}
