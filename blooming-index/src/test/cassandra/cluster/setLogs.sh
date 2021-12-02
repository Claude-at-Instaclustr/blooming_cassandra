if [ -z $1 ]
then
	level=DEBUG
else
	level=$1
fi
docker exec -it cluster_cassandra-one_1 bash -c "export NO_DEBUG=TRUE;nodetool setlogginglevel -- com.instaclustr.cassandra $level;nodetool getlogginglevels"
docker exec -it cluster_cassandra-two_1 bash -c "export NO_DEBUG=TRUE;nodetool setlogginglevel -- com.instaclustr.cassandra $level;nodetool getlogginglevels"
docker exec -it cluster_cassandra-three_1 bash -c "export NO_DEBUG=TRUE;nodetool setlogginglevel -- com.instaclustr.cassandra $level;nodetool getlogginglevels"

