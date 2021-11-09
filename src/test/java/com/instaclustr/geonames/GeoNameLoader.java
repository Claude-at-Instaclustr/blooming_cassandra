package com.instaclustr.geonames;

import java.util.Iterator;
import java.util.function.Consumer;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.instaclustr.cassandra.bloom.BulkExecutor;

public class GeoNameLoader {

    public static void load( GeoNameIterator iter, Session session) {
        load( iter, session, null);
    }

    public static void load( GeoNameIterator iter, Session session, Consumer<GeoName> consumer ) {
        BulkExecutor bulkExecutor = new BulkExecutor( session );
        Consumer<GeoName> writer = new Consumer<GeoName>() {

            @Override
            public void accept(GeoName gn) {
                try {
                    bulkExecutor.execute( GeoName.CassandraSerde.serialize(gn ));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        };
        if (consumer != null) {
            writer = writer.andThen(consumer);
        }
        iter.forEachRemaining( writer );

        bulkExecutor.awaitFinish();
    }


}
