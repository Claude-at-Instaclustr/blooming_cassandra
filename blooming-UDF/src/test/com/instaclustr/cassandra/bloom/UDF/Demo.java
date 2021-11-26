package com.instaclustr.cassandra.bloom.UDF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.HasherCollection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.instaclustr.geonames.GeoName;
import com.instaclustr.geonames.GeoNameHasher;
import com.instaclustr.geonames.GeoNameIterator;
import com.instaclustr.geonames.GeoNameLoader;

public class Demo {

    private Cluster cluster;
    private Session session;

    private static final String keyspace = "CREATE KEYSPACE IF NOT EXISTS geoNames WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    private static final String table = "CREATE TABLE geoNames.geoname (geonameid text, name text, asciiname text, alternatenames text, latitude text, longitude text, feature_class text,feature_code text,country_code text,cc2 text,admin1_code text,admin2_code text, admin3_code text, admin4_code text, population text, elevation text, dem text, timezone text, modification_date text,bf blob,PRIMARY KEY (geonameid ));";
    private static final String func = "CREATE OR REPLACE FUNCTION  "
            + "geoNames.BFContains (candidate Blob, target Blob) "
            + "CALLED ON NULL INPUT "
            + "RETURNS boolean "
            + "LANGUAGE Java "
            + "AS '"
            + "    if (candidate == null) { "
            + "        return target==null?Boolean.TRUE:Boolean.FALSE; "
            + "    } "
            + "    if (target == null) { "
            + "        return Boolean.TRUE; "
            + "    } "
            + "     "
            + "    if (candidate.remaining() < target.remaining() ) { "
            + "        return Boolean.FALSE; "
            + "    } "
            + "    while (target.remaining()>0) { "
            + "        byte b = target.get(); "
            + "        if ((candidate.get() & b) != b) { "
            + "            return Boolean.FALSE; "
            + "                } "
            + "        } "
            + "        return Boolean.TRUE; "
            + "';";

    public Demo() {
        Cluster.Builder builder = Cluster.builder()
                .addContactPoint( "localhost");
        cluster = builder.build();
        session = cluster.connect();

    }

    public void initTable() {
        session.execute(keyspace);
        session.execute(table);
        session.execute( func );
    }

    public void load( URL url ) throws IOException {
        GeoNameIterator iter = new GeoNameIterator(url);
        GeoNameLoader.load(iter, session);
    }

    /*public ResultSet search( BloomFilter filter ) {
        return session.execute( GeoName.CassandraSerde.query(filter) );
    }
*/
    public static void main(String[] args) throws IOException {
        Demo demo = new Demo();
            demo.initTable();
            demo.load( GeoNameIterator.DEFAULT_INPUT);
        }

}
