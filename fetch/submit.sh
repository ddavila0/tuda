#!/bin/bash
echo $@
source /cvmfs/sft.cern.ch/lcg/views/LCG_93/x86_64-centos7-gcc62-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh analytix
spark-submit \
    --conf spark.driver.port=5001 \
    --conf spark.blockManager.port=5101 \
    --conf spark.ui.port=5201 \
    --conf spark.driver.extraClassPath='/eos/project/s/swan/public/hadoop-mapreduce-client-core-2.6.0-cdh5.7.6.jar' \
    ${1} --datemin="${2}" --datemax="${3}"
