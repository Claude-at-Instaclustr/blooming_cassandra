# Blooming Cassandra
A project to add Bloom filter indexing (Multidimensional Bloom Filters) to Cassandra.

This project depends upon Bloom filter libraries not currently in the Apache Commons collections libraries.  
You will need to clone `https://github.com/Claudenw/commons-collections` and build the `simplify-bloom-filters` branch

```
git clone https://github.com/Claudenw/commons-collections
cd commons-collections
git fetch --all
git checkout simplify_bloom_filters
mvn install
```


## Test setup

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

### Docker compose

The `docker-compose.yml` will start a single docker based Cassandra instance.

The `cassandra.yaml` will be loaded to configure the Cassandra instance.  The current (9 Nov 2021) cassandra yaml file allows for the creation of Java based UDFs.

### Test execution

Compile the src/test/java/com/instaclustr/cassandra/bloom/idx/Demo.java file.  Execute the `Demo` application with a command line argument to read the data.

Execute the `Demo` application with no command line arguments to start and use the already populated localhost based Cassandra.

