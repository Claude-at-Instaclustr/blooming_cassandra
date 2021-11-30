# Blooming Cassandra

A project to add Bloom filter indexing (multidimensional Bloom filters) to Cassandra.  The code utilizes a chunked "Flat-Bloofi" design.

## Project Layout

This is a multi-module Maven project.  

### blooming-test-helpers

This module contains helper classes that are used throughout testing including the code to read and write GeoNames objects and a version of the Apache Jena iterator utilities modified to properly close Cassandra iterators.

### blooming-UDF

This modules contains a simple UDF to perform Bloom filter comparisons on the server side.  It is of limited use as UDFs can not be used in `WHERE` clauses, but it does show how the standard Bloom filter comparison can be implemented on the server side.

### blooming-table

This module contains a a proof-of-concept for the index table. In this implementation IdxTable is the multidimensional Bloom filter and uses the the `GeoNames.geonameid` as the index.  The test code includes a Demo that shows how to load and query the data.

All Cassandra interaction is client side so this implementation is fairly slow.

### blooming-index

This module contains an implementation of a multidimensional Bloom filter as a Cassandra secondary index.  It utilizes the same storage strategy as the `blooming-table` code but does so on the server side.

## Development Configuration

This project depends upon Bloom filter libraries not currently in the Apache Commons collections libraries.  
You will need to clone `https://github.com/Claudenw/commons-collections` and build the `simplify-bloom-filters` branch

```
git clone https://github.com/Claudenw/commons-collections
cd commons-collections
git fetch --all
git checkout simplify_bloom_filters
mvn install
```

### Test setup

In order to load the GeoNames test code you will need to download the and unzip the geonames data.
As of 9 Nov 2021 there are 12,214,535 entries in the allCountries data.

```
cd src/test/resources
curl -o allCountries.zip https://download.geonames.org/export/dump/allCountries.zip
unzip allCountries.zip
```

The `allCountries` files are not checked into github.  
As of 9 Nov 2021 there are 12,214,535 entries in the allCountries data.
You can reduce the number of geonames to create by executing 

```
cp allCountries.txt allCountries.bak
head --lines 1000 allCountries.bak > allCountries.txt
```

The above will create an allCountries.txt that contains the first 1000 entries.

#### Docker compose

The `docker-compose.yml` will start a single docker based Cassandra instance.

The `cassandra.yaml` will be loaded to configure the Cassandra instance.  The current (9 Nov 2021) cassandra yaml file allows for the creation of Java based UDFs.

#### Test execution

Compile the src/test/java/com/instaclustr/cassandra/bloom/idx/Demo.java file.  Execute the `Demo` application with a command line argument to read the data.

Execute the `Demo` application with no command line arguments to start and use the already populated localhost based Cassandra.

## Multidimensional Bloom Filters -- an Overview

Multidimensional Bloom filters are a collection of Bloom filters.  The simplest form is a list of Bloom filters that are scanned checked on each matching request.  This is by far the easiest to implement and the most effecient for small (`n` < 1000) collections of filters.  However, as the number of filters grows the time necessary to scan the list becomes excessive.  

Other implementations of multidimensional Bloom filters use modified B+ trees or Trie structures.  These work for indexing Bloom filters under certain conditions.  The solution that works the best across all Bloom filter conditions is the Flat-Bloofi design.

Flat-Bloofi uses a matrix approach.  If each bit position in the Bloom filter is a column and each Bloom filter is a row in a matrix. To find matching bloom filters for a target filter each column of the matrix that corresponds to an enabled bit is in the target is scanned and the row indexes of all the rows that have that bit enabled are collected into a set. The intersection of the sets is the set of Bloom filters that match.

The chunked Flat-Bloofi is better opimised for database storage.  Instead of treating each bit separately this code considers bytes.  On storage the Bloom filter is decomposed to a byte array and each non-zero byte, its array position and the identifier for the Bloom filter is then written to an index table.  When searching the target Bloom filter is decomposed to a byte array and each non-zero byte is expanded to the set of all "matching" bytes, where "matching" is defined as the Bloom filter matching algorithm.  The index table is then queried for all matching Bloom filters for the matching bytes at the index position.  The result of this query is the set of all Bloom filters that have the bits enabled in the target byte and position.  The intersection of the sets is then calculated and the result is the list of matching Bloom filters.

An optimisation strategy is utilized where each byte is given a selectivity value based on the number of other bytes it matches.  For example 0xFF will only match 0xFF while 0xFE will match 0xFF and 0xFE.  So 0xFF is more selective than 0xFE.  Zeror (0x00) always matches everthing and is thus removed from the algorithm discussed above.  The optimisation sorts the positional data by selectivity and performs the most selective queries first.  This produces the smallest initial solution sets and should detect no solution conditions early.

### References

Crainiceanu, Adina and Lemire, Daniel, "[Bloofi: Multidimensional Bloom filters](https://arxiv.org/abs/1501.01941)", Information Systems, v54, pp 311-325, Dec 2015.

Bloom, Burton H., "[Space/Time Trade-offs in Hash Coding with Allowable Errors](https://www.cs.princeton.edu/courses/archive/spr05/cos598E/bib/p422-bloom.pdf)", Communications of the ACM, v13 n7, pp 422–426, July 1970.
