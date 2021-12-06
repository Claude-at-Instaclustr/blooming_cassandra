#!/bin/bash
#
cd `dirname $0`

#./cleanup.sh
docker-compose up -d
cnt=`docker-compose logs | grep -c "Startup complete"`

echo "Waiting for 'Startup complete'"
while [ $cnt -lt 3 ]
do
	sleep 1
	cnt=`docker-compose logs | grep -c "Startup complete"`
done

./setLogs.sh

