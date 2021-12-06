# Blooming Index

This module contains an implementation of a multidimensional Bloom filter as a Cassandra secondary index.  It utilizes the same storage strategy as the `blooming-table` code but does so on the server side.

## Usage

Execute `mvn mvn pre-integration-test` to create the a docker Cassandra image with the index included.  

### Starting a single node Cassandra

To start a single Cassandra instance use the scripts in the `src/test/cassandra` directory:

 * `./start.sh` will start the server and announce when the startup is completed.  All data will be stored in the `src/test/cassandra/data` directory so that it will be preserved from run to run.  All log data will be stored in `src/test/cassandra/logs`.  If `start.sh` is passed a parameter it starts the docker container in debug mode and will require a java debugging system to attach on port 8000.
 * `./stop.sh` to stop the server.
 * `/cleanup.sh` will delete the data and logs.
 * `./setLogs.sh` will set the log level for the indexing code.  Without parameters it sets the level to DEBUG.  All Cassandra logging levels are accepted. 

### Starting a Cassandra cluster

To start a Cassandra clauster use the scripts in the `src/test/cassandra/cluster` directory:

 * `./start.sh` will start the server and announce when the startup is completed.  All data will be stored in the `src/test/cassandra/data?` directory so that it will be preserved from run to run.  All log data will be stored in `src/test/cassandra/logs?`. 
 * `./stop.sh` to stop the server.
 * `/cleanup.sh` will delete the data and logs.
 * `./setLogs.sh` will set the log level for the indexing code.  Without parameters it sets the level to DEBUG.  All Cassandra logging levels are accepted. 


Once Cassandra is started execute:

```
CREATE CUSTOM INDEX IF NOT EXISTS ON <table>(<bfColumn>) USING 'com.instaclustr.cassandra.bloom.idx.std.BloomingIndex'"
            + " WITH OPTIONS = {'numberOfBits':'<m>', 'numberOfItems':'<n>', 'numberOfFunctions':'<k>' }";

```
Where: 
 * `<table>` is the table to put the index on.
 * `<bfColumn>` is the column in the table the contains the Bloom filter.
 * `<m>` is the number of bits in the bloom filter.
 * `<n>` is the average number of items added to a Bloom filter in the column.
 * `<k>` is the number of hash functions applied to each item added to the Bloom filter.
 
 If `m`, `n`, or `k`, is unknown none of them should be specified.
 
 To use the index simply select from the table where the column equals a hex encoded Bloom filter.
 For example if using the data loaded by the `Demo` application (see below) executing:
 
 ```
 SELECT * 
 FROM geoNames.geoname 
 WHERE filter = 0x80002020000A000000800020000A0000008000200008020080802000080002000000200008000200 ;
 ```
 will return all the records that have 'Xixerella' in the name field as well as seveal false positives.
         
## Demo

Compile the src/test/java/com/instaclustr/cassandra/bloom/idx/Demo.java file.  Execute the `Demo` application with a command line argument to read the data.

Execute the `Demo` application with no command line arguments to start and use the already populated localhost based Cassandra.

When running the demo enter data from the GeoNames data `name`, `asciiname`, `feature_code`,
`country_code`, or the first 10 `alternatenames` fields.

Press enter on a blank line and all the matching entries will be printed.  This example does not filter out false positives.
