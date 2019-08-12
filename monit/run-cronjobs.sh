#!/bin/bash
container=`docker ps --format "{{.ID}}"`
docker exec ${container} /bin/bash "/monicron/run-jobs.sh"
