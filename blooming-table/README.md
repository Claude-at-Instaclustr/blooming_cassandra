# Blooming Table

This module contains a a proof-of-concept for the index table. In this implementation IdxTable is the multidimensional Bloom filter and uses the the `GeoNames.geonameid` as the index.  The test code includes a Demo that shows how to load and query the data.

All Cassandra interaction is client side so this implementation is fairly slow.

## Usge


## Docker compose

The `docker-compose.yml` will fpidm om `src/text/cassandra` start a single docker based Cassandra instance.

All data will be saved in the `src/test/cassandra/data` directory so that the docker container my be restarted without having to reload all the example data.


#### Test execution

Compile the src/test/java/com/instaclustr/cassandra/bloom/idx/Demo.java file.  Execute the `Demo` application with a command line argument to read the data.

Execute the `Demo` application with no command line arguments to start and use the already populated localhost based Cassandra.

When running the demo enter data from the GeoNames data `name`, `asciiname`, `feature_code`,
`country_code`, or the first 10 `alternatenames` fields.

Press enter on a blank line and all the matching entries will be printed.  This example does not filter out false positives.
