container=`docker run -d -it -p 5001-5300:5001-5300 --hostname tuda@cern.ch gitlab-registry.cern.ch/db/cerndb-infra-hadoop-conf:master`
docker cp monicron.py ${container}:/home/
