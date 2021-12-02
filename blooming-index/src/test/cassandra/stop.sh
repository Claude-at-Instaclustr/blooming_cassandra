#!/bin/bash
#
cd `dirname $0`

if [ -z $1 ]
then
        DOCKER_PARMS=""
else
        DOCKER_PARMS="-f docker-compose-dbg.yml"
fi

docker-compose ${DOCKER_PARMS} down
