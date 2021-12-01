package com.instaclustr.geonames;

import java.util.function.Consumer;

import com.datastax.driver.core.Session;
import com.instaclustr.cassandra.BulkExecutor;

public class GeoNameLoader {

    public static void load(GeoNameIterator iter, Session session, String table) {
        load(iter, session, table, null);
    }

    public static void load(GeoNameIterator iter, Session session, String table, Consumer<GeoName> consumer) {
        BulkExecutor bulkExecutor = new BulkExecutor(session);
        Consumer<GeoName> writer = new Consumer<GeoName>() {

            @Override
            public void accept(GeoName gn) {
                try {
                    bulkExecutor.execute(String.format( GeoName.CassandraSerde.serialize(gn), table ));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        };
        if (consumer != null) {
            writer = writer.andThen(consumer);
        }
        iter.forEachRemaining(writer);

        bulkExecutor.awaitFinish();
    }

}
