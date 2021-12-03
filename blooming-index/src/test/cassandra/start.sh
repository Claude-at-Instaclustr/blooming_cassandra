#!/bin/bash
#
cd `dirname $0`

if [ -z "$1" ] 
then
	DOCKER_PARAMS=""
else
	DOCKER_PARAMS="-f docker-compose-dbg.yml"
fi

docker-compose ${DOCKER_PARAMS} up -d
cnt=`docker-compose ${DOCKER_PARAMS} logs | grep -c "Startup complete"`
if [ -n "$1" ]
then
	echo "Start debugger"
fi
echo "Waiting for 'Startup complete'"
while [ $cnt -lt 1 ]
do
	sleep 1
	cnt=`docker-compose ${DOCKER_PARAMS} logs | grep -c "Startup complete"`
done

./setLogs.sh

#docker-compose ${DOCKER_PARAMS} logs -f
