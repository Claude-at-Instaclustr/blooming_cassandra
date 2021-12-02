if [ -z $1 ]
then
	level=DEBUG
else
	level=$1
fi
docker exec -it cassandra_cassandra-one_1 bash -c "export NO_DEBUG=true; nodetool setlogginglevel -- com.instaclustr.cassandra $level;nodetool getlogginglevels"


