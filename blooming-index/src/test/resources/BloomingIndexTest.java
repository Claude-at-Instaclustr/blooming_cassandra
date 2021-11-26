/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.internal;


import com.instaclustr.cassandra.bloom.idx.std.BitMap;
import com.instaclustr.cassandra.bloom.idx.std.BloomingIndex;
import com.instaclustr.geonames.GeoName;
import com.instaclustr.geonames.GeoNameHasher;
import com.instaclustr.geonames.GeoNameIterator;

import org.junit.Test;
import org.junit.After;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.HasherCollection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

public class BloomingIndexTest extends CQLTester
{
    private static final String table = "CREATE TABLE %s (geonameid text, name text, asciiname text, alternatenames text, latitude text, longitude text, feature_class text,feature_code text,country_code text,cc2 text,admin1_code text,admin2_code text, admin3_code text, admin4_code text, population text, elevation text, dem text, timezone text, modification_date text,filter blob,PRIMARY KEY (geonameid ));";
    private static final String index = "CREATE CUSTOM INDEX ON %s(filter) USING 'com.instaclustr.cassandra.bloom.idx.std.BloomingIndex'"
            + " WITH OPTIONS = {'numberOfBits':'302', 'numberofItems':'10', 'numberOfFunctions':'21' }";

    private static final String query = "SELECT * FROM %%s WHERE filter = %s";


    public BloomingIndexTest() {
    }

    private static void assertEqual(BloomFilter expected, BloomFilter actual) {

        byte[] exectedData = extractBytes( expected );
        byte[] actualData = extractBytes( actual );
        Object[] diffResult = diff( exectedData, actualData );
        if( (Boolean)diffResult[0] ) {
            // here are changes so report them
            StringBuilder sb = new StringBuilder( String.format("Bloom filters differ %n"));

            for (int i=0;i<(Integer)diffResult[1];i++) {
                if (BitMap.contains( (long[])diffResult[2], i)) {
                    sb.append( String.format(" -> changed %d from 0x%02x to 0x%02x %n", i,
                            exectedData[i], actualData[i] ));
                }
            }
            fail( sb.toString() );
        }
    }

    private static void assertEqual( GeoName expected, GeoName actual ) {
        assertEquals( expected.admin1_code, actual.admin1_code );
        assertEquals( expected.admin2_code, actual.admin2_code );
        assertEquals( expected.admin3_code, actual.admin3_code );
        assertEquals( expected.admin4_code, actual.admin4_code );
        assertEquals( expected.alternatenames, actual.alternatenames );
        assertEquals( expected.asciiname, actual.asciiname );
        assertEquals( expected.cc2, actual.cc2 );
        assertEquals( expected.country_code, actual.country_code );
        assertEquals( expected.dem, actual.dem );
        assertEquals( expected.elevation, actual.elevation );
        assertEquals( expected.feature_class, actual.feature_class );
        assertEquals( expected.feature_code, actual.feature_code );
        assertEquals( expected.geonameid, actual.geonameid );
        assertEquals( expected.latitude, actual.latitude );
        assertEquals( expected.longitude, actual.longitude );
        assertEquals( expected.modification_date, actual.modification_date );
        assertEquals( expected.name, actual.name );
        assertEquals( expected.population, actual.population );
        assertEquals( expected.timezone, actual.timezone );
        // now test the filters
        assertEqual( expected.filter, actual.filter );

    }

    @Test
    public void testOptionParser() {
        Map<String,String> options = new HashMap<String,String>();
        int i = BloomingIndex.parseInt(options, "missing");
        assertEquals( 0, i );
        options.put( "one", "1" );
        i = BloomingIndex.parseInt(options, "one");
        assertEquals( 1, i );
        options.put( "float", "2.3");
        try {
            BloomingIndex.parseInt(options, "float");
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // do nothing.
        }
    }

    private void setupTables() {

        createTable(table);
        createIndex(index);
    }


    @Test
    public void testInsertAndRetrieval() throws Throwable
    {
        setupTables();

        GeoNameIterator iter = new GeoNameIterator( GeoNameIterator.DEFAULT_INPUT );
        GeoName[] gn = new GeoName[3];
        gn[0] = iter.next();
        gn[1] = iter.next();
        gn[2] = iter.next();
        iter.close();

        String s = GeoName.CassandraSerde.serialize( gn[0] );
        System.out.println( "Inserting "+gn[0].geonameid );
        execute( formatQuery(s) );
        System.out.println( "Inserting "+gn[1].geonameid );
        execute( formatQuery(GeoName.CassandraSerde.serialize( gn[1] )));
        System.out.println( "Inserting "+gn[2].geonameid );
        execute( formatQuery(GeoName.CassandraSerde.serialize( gn[2] )));
        System.out.println( "Done" );
        flush();

/*
        UntypedResultSet results = execute(String.format(query, GeoName.CassandraSerde.hexString(gn[2].filter)));
        assertFalse( results.isEmpty() );
        assertEquals( 1, results.size());
        GeoName result = GeoName.CassandraSerde.deserialize( results.one() );
        assertEqual( gn[2], result );

        results = execute(String.format(query, GeoName.CassandraSerde.hexString(gn[1].filter)));
        assertFalse( results.isEmpty() );
        assertEquals( 1, results.size());
        result = GeoName.CassandraSerde.deserialize( results.one() );

        System.out.println( gn[1] );
        System.out.println( result );

        assertEqual( gn[1], result );

        results = execute(String.format(query, GeoName.CassandraSerde.hexString(gn[0].filter)));
        assertFalse( results.isEmpty() );
        assertEquals( 1, results.size());
        result = GeoName.CassandraSerde.deserialize( results.one() );
        assertEqual( gn[0], result );

        BloomFilter test = new SimpleBloomFilter( GeoNameHasher.shape, GeoNameHasher.hasherFor( "Pic de Font Blanca") );
        results = execute(String.format(query, GeoName.CassandraSerde.hexString(test)));
        assertFalse( results.isEmpty() );
        assertEquals( 1, results.size());
        result = GeoName.CassandraSerde.deserialize( results.one() );
        assertEqual( gn[0], result );
*/
    }

    @Test
    public void testUpdate() throws Throwable
    {
        setupTables();

        GeoNameIterator iter = new GeoNameIterator( GeoNameIterator.DEFAULT_INPUT );
        GeoName[] gn = new GeoName[3];
        gn[0] = iter.next();
        gn[1] = iter.next();
        gn[2] = iter.next();
        iter.close();

        String s = GeoName.CassandraSerde.serialize( gn[0] );
        System.out.println( "Inserting "+gn[0].geonameid );
        execute( formatQuery(s) );
        System.out.println( "Inserting "+gn[1].geonameid );
        execute( formatQuery(GeoName.CassandraSerde.serialize( gn[1] )));
        System.out.println( "Inserting "+gn[2].geonameid );
        execute( formatQuery(GeoName.CassandraSerde.serialize( gn[2] )));
        System.out.println( "Done" );
        flush();

        // change the hashed data
        HasherCollection hasher = new HasherCollection();
        hasher.add( GeoNameHasher.hasherFor( gn[1].name+" Changed"));
        hasher.add( GeoNameHasher.hasherFor( gn[1].asciiname+" Changed"));
        hasher.add( GeoNameHasher.hasherFor( gn[1].feature_code));
        hasher.add( GeoNameHasher.hasherFor( gn[1].country_code));
        // exclude the alternate names

        BloomFilter newFilter = new SimpleBloomFilter( GeoNameHasher.shape, hasher );

        execute(String.format("UPDATE %%s SET filter = %s WHERE geonameid = $$%s$$",
                GeoName.CassandraSerde.hexString(newFilter), gn[1].geonameid));
        flush();
/*
        BloomFilter test = new SimpleBloomFilter( GeoNameHasher.shape, GeoNameHasher.hasherFor( gn[1].name+" Changed") );
        UntypedResultSet results = execute(String.format(query, GeoName.CassandraSerde.hexString(test)));
        assertFalse( results.isEmpty() );
        assertEquals( 1, results.size());
        GeoName result = GeoName.CassandraSerde.deserialize( results.one() );
        gn[1].filter = newFilter;
        assertEqual( gn[1], result );
*/
    }


    @Test
    public void testDelete() throws Throwable
    {
        setupTables();

        GeoNameIterator iter = new GeoNameIterator( GeoNameIterator.DEFAULT_INPUT );
        GeoName[] gn = new GeoName[3];
        gn[0] = iter.next();
        gn[1] = iter.next();
        gn[2] = iter.next();
        iter.close();

        String s = GeoName.CassandraSerde.serialize( gn[0] );
        System.out.println( "Inserting "+gn[0].geonameid );
        execute( formatQuery(s) );
        System.out.println( "Inserting "+gn[1].geonameid );
        execute( formatQuery(GeoName.CassandraSerde.serialize( gn[1] )));
        System.out.println( "Inserting "+gn[2].geonameid );
        execute( formatQuery(GeoName.CassandraSerde.serialize( gn[2] )));
        System.out.println( "Done" );
        flush();

        execute(String.format("DELETE FROM %%s WHERE geonameid = $$%s$$", gn[1].geonameid));
        flush();
/*
        UntypedResultSet results =  execute(String.format(query, GeoName.CassandraSerde.hexString(gn[1].filter)));
        assertTrue( results.isEmpty() );
        */
    }


    private static byte[] extractBytes(BloomFilter filter) {

        long[] buff = BloomFilter.asBitMapArray(filter);
        byte[] codes = new byte[buff.length*Long.BYTES];
        int pos=0;
        for (long word : buff) {
            for (int i = 0; i < Long.BYTES; i++) {
                byte code = (byte) (word & 0xFF);
                word = word >> Byte.SIZE;
            codes[pos++] = code;
            }
        }
        return codes;

    }

    private static Object[] diff( byte[] oldBytes, byte[] newBytes ) {
        int limit = oldBytes.length > newBytes.length ? oldBytes.length : newBytes.length;
        int min = oldBytes.length > newBytes.length ? newBytes.length : oldBytes.length;
        boolean changed = false;

        long[] changes = new long[BitMap.numberOfBitMaps(limit)];
        for (int i = 0; i < min; i++) {
            if (oldBytes[i] != newBytes[i]) {
                BitMap.set(changes, i);
                changed = true;
            }
        }
        for (int i = min; i < limit; i++) {
            BitMap.set(changes, i);
            changed = true;
        }
        return new Object[] {changed, limit,changes};
    }

}
