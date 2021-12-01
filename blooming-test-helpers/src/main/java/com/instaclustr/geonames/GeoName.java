package com.instaclustr.geonames;

import java.nio.LongBuffer;
import java.util.function.LongConsumer;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.Hasher;

import com.datastax.driver.core.Row;

/*
 The main 'geoname' table has the following fields :

geonameid         : integer id of record in geonames database
name              : name of geographical point (utf8) varchar(200)
asciiname         : name of geographical point in plain ascii characters, varchar(200)
alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
latitude          : latitude in decimal degrees (wgs84)
longitude         : longitude in decimal degrees (wgs84)
feature class     : see http://www.geonames.org/export/codes.html, char(1)
feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
country code      : ISO-3166 2-letter country code, 2 characters
cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
admin3 code       : code for third level administrative division, varchar(20)
admin4 code       : code for fourth level administrative division, varchar(20)
population        : bigint (8 byte int)
elevation         : in meters, integer
dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
timezone          : the timezone id (see file timeZone.txt) varchar(40)
modification date : date of last modification in yyyy-MM-dd format
 */
public class GeoName {
    public String geonameid;
    public String name;
    public String asciiname;
    public String alternatenames;
    public String latitude;
    public String longitude;
    public String feature_class;
    public String feature_code;
    public String country_code;
    public String cc2;
    public String admin1_code;
    public String admin2_code;
    public String admin3_code;
    public String admin4_code;
    public String population;
    public String elevation;
    public String dem;
    public String timezone;
    public String modification_date;
    public BloomFilter filter;

    @Override
    public String toString() {
        return new StringBuilder().append("ID: ").append(geonameid).append("\n").append("Name:").append(name)
                .append("\n").append("Ascii name: ").append(asciiname).append("\n").append("Alternate names: ")
                .append(alternatenames).append("\n").append("Latitude: ").append(latitude).append("\n")
                .append("Longitude: ").append(longitude).append("\n").append("Feature class: ").append(feature_class)
                .append("\n").append("Feature code: ").append(feature_code).append("\n").append("Country code: ")
                .append(country_code).append("\n").append("Country code2: ").append(cc2).append("\n")
                .append("Admin code1: ").append(admin1_code).append("\n").append("Admin code2: ").append(admin2_code)
                .append("\n").append("Admin code3: ").append(admin3_code).append("\n").append("Admin code4: ")
                .append(admin4_code).append("\n").append("Population: ").append(population).append("\n")
                .append("Elevation: ").append(elevation).append("\n").append("Dem: ").append(dem).append("\n")
                .append("Timezone: ").append(timezone).append("\n").append("Modification date: ")
                .append(modification_date).toString().toString();
    }

    public static class Serde {

        // do not instantiate
        private Serde() {
        }

        public static GeoName deserialize(String txt) {
            String[] parts = txt.split("\t");
            if (parts.length != 19) {
                System.out.println("too short");
            }
            GeoName retval = new GeoName();
            retval.geonameid = parts[0];
            retval.name = parts[1];
            retval.asciiname = parts[2];
            retval.alternatenames = parts[3];
            retval.latitude = parts[4];
            retval.longitude = parts[5];
            retval.feature_class = parts[6];
            retval.feature_code = parts[7];
            retval.country_code = parts[8];
            retval.cc2 = parts[9];
            retval.admin1_code = parts[10];
            retval.admin2_code = parts[11];
            retval.admin3_code = parts[12];
            retval.admin4_code = parts[13];
            retval.population = parts[14];
            retval.elevation = parts[15];
            retval.dem = parts[16];
            retval.timezone = parts[17];
            retval.modification_date = parts[18];
            Hasher hasher = GeoNameHasher.createHasher(retval);
            retval.filter = new SimpleBloomFilter(GeoNameHasher.shape, hasher);
            return retval;
        }

        public static String serialize(GeoName geoname) {
            return new StringBuffer(geoname.geonameid).append("\t").append(geoname.name).append("\t")
                    .append(geoname.asciiname).append("\t").append(geoname.alternatenames).append("\t")
                    .append(geoname.latitude).append("\t").append(geoname.longitude).append("\t")
                    .append(geoname.feature_class).append("\t").append(geoname.feature_code).append("\t")
                    .append(geoname.country_code).append("\t").append(geoname.cc2).append("\t")
                    .append(geoname.admin1_code).append("\t").append(geoname.admin2_code).append("\t")
                    .append(geoname.admin3_code).append("\t").append(geoname.admin4_code).append("\t")
                    .append(geoname.population).append("\t").append(geoname.elevation).append("\t").append(geoname.dem)
                    .append("\t").append(geoname.timezone).append("\t").append(geoname.modification_date).toString();
        }
    }

    public static class CassandraSerde {

        private static String fmt = "INSERT INTO %%s (geonameid, name, asciiname, alternatenames, latitude, longitude, "
                + "feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, "
                + "population, elevation, dem, timezone, modification_date, filter ) VALUES "
                + "($$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,$$%s$$,%s);";

        private static String qry = "SELECT geonameid, name, asciiname, alternatenames, latitude, longitude, "
                + "feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, "
                + "population, elevation, dem, timezone, modification_date FROM geonames.GeoName WHERE geonameid='%s'";

        // do not instantiate
        private CassandraSerde() {
        }

        public static String hexString(BloomFilter filter) {
            StringBuilder sb = new StringBuilder("0x");
            filter.forEachBitMap(word -> sb.append(String.format("%016X", word)));
            return sb.toString();
        }

        public static String query(String geonameid) {
            return String.format(GeoName.CassandraSerde.qry, geonameid);
        }

        public static GeoName deserialize(Row row) {
            GeoName result = new GeoName();
            result.geonameid = row.getString(0);
            result.name = row.getString(1);
            result.asciiname = row.getString(2);
            result.alternatenames = row.getString(3);
            result.latitude = row.getString(4);
            result.longitude = row.getString(5);
            result.feature_class = row.getString(6);
            result.feature_code = row.getString(7);
            result.country_code = row.getString(8);
            result.cc2 = row.getString(9);
            result.admin1_code = row.getString(10);
            result.admin2_code = row.getString(11);
            result.admin3_code = row.getString(12);
            result.admin4_code = row.getString(13);
            result.population = row.getString(14);
            result.elevation = row.getString(15);
            result.dem = row.getString(16);
            result.timezone = row.getString(17);
            result.modification_date = row.getString(18);
            Hasher hasher = GeoNameHasher.createHasher(result);
            result.filter = new SimpleBloomFilter(GeoNameHasher.shape, hasher);
            return result;
        }

        public static String serialize(GeoName geoname) {
            return String.format(fmt, geoname.geonameid, geoname.name, geoname.asciiname, geoname.alternatenames,
                    geoname.latitude, geoname.longitude, geoname.feature_class, geoname.feature_code,
                    geoname.country_code, geoname.cc2, geoname.admin1_code, geoname.admin2_code, geoname.admin3_code,
                    geoname.admin4_code, geoname.population, geoname.elevation, geoname.dem, geoname.timezone,
                    geoname.modification_date, hexString(geoname.filter));
        }

        public static GeoName deserialize(org.apache.cassandra.cql3.UntypedResultSet.Row row) {
            GeoName result = new GeoName();
            result.geonameid = row.getString("geonameid");
            result.name = row.getString("name");
            result.asciiname = row.getString("asciiname");
            result.alternatenames = row.getString("alternatenames");
            result.latitude = row.getString("latitude");
            result.longitude = row.getString("longitude");
            result.feature_class = row.getString("feature_class");
            result.feature_code = row.getString("feature_code");
            result.country_code = row.getString("country_code");
            result.cc2 = row.getString("cc2");
            result.admin1_code = row.getString("admin1_code");
            result.admin2_code = row.getString("admin2_code");
            result.admin3_code = row.getString("admin3_code");
            result.admin4_code = row.getString("admin4_code");
            result.population = row.getString("population");
            result.elevation = row.getString("elevation");
            result.dem = row.getString("dem");
            result.timezone = row.getString("timezone");
            result.modification_date = row.getString("modification_date");
            result.filter = new SimpleBloomFilter(GeoNameHasher.shape, new BitMapProducer() {

                @Override
                public void forEachBitMap(LongConsumer consumer) {
                    LongBuffer lb = row.getBlob("filter").asLongBuffer();
                    while (lb.hasRemaining()) {
                        consumer.accept(lb.get());
                    }
                }
            });
            return result;
        }
    }

}
