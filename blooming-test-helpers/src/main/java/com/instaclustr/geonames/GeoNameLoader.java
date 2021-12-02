package com.instaclustr.geonames;

import java.util.Iterator;
import java.util.function.Consumer;

import com.datastax.driver.core.Session;
import com.instaclustr.cassandra.BulkExecutor;

public class GeoNameLoader {

    public static void load(Iterator<GeoName> iter, BulkExecutor bulkExecutor, String table) {
        load(iter, bulkExecutor, table, null);
    }

    public static void load(Iterator<GeoName> iter, BulkExecutor bulkExecutor,  String table, Consumer<GeoName> consumer) {
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
