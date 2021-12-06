# Blooming-UDF

This modules contains a simple user defined function (UDF) to perform Bloom filter comparisons on the server side.  It is of limited use as UDFs can not be used in `WHERE` clauses, but it does show how the standard Bloom filter comparison can be implemented on the server side.

## Usage

To use the UDF connect to a Cassandra that has been configured to permit user defined functions and execute:

```
CREATE OR REPLACE FUNCTION  
    <keyspace>.BFContains (candidate Blob, target Blob) 
    CALLED ON NULL INPUT 
    RETURNS boolean "
    LANGUAGE Java "
    AS '
        if (candidate == null) { 
            return target==null?Boolean.TRUE:Boolean.FALSE; 
        } 
        if (target == null) { 
            return Boolean.TRUE; 
        } 
         
        if (candidate.remaining() < target.remaining() ) { 
            return Boolean.FALSE; 
        } 
        while (target.remaining()>0) { 
            byte b = target.get(); 
            if ((candidate.get() & b) != b) { 
                return Boolean.FALSE; 
                    } 
            } 
            return Boolean.TRUE; 
    ';
```
where `<keyspace>` is the keyspace you want the function defined in.

To call the function execute:

`SELECT BFContains( <bfColumn>, <bfValue> ) FROM <table> WHERE ... `

where:
 * `bfColumn` is a column in `table` that contains bloom filter blobs.
 * `bfVCalue` is the literal Cassandra blob value to find in `table`.
 
If using the table created by the Demo application the following query should work.

```
SELECT geonameid, name, BFContains( filter, 0x0x80002020000A000000800020000A0000008000200008020080802000080002000000200008000200 ) 
FROM geonames.geoname
WHERE geonameid<'3038817' AND geonameid>'303800' 
ALLOW FILTERING;
```

This should return several geonameids along with their name and `true` or `false` if they match the filter.  In the example data the ones with the name 'Xixerella' should be marked as `true`

## Docker compose

The `docker-compose.yml` in the `src/test/cassandra` directory will start a single docker based Cassandra instance.

The `cassandra.yaml` will be loaded to configure the Cassandra instance.  This `cassandra.yaml` file allows for the creation of Java based UDFs.

All data will be saved in the `src/test/cassandra/data` directory so that the docker container my be restarted without having to reload all the example data.
