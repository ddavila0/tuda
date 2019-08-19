#!/bin/bash
container=`docker ps --format "{{.ID}}"`
docker exec ${container} sh -c "/monicron/run-jobs.sh ${1}"
