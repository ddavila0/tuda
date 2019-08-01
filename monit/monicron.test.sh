#!/bin/bash
for config in ./configs/*.json; do
    echo "Running aggregations for ${config}"
    spark-submit \
        --conf spark.driver.port=5001 \
        --conf spark.blockManager.port=5101 \
        --conf spark.ui.port=5201 \
        --conf spark.driver.extraClassPath='/eos/project/s/swan/public/hadoop-mapreduce-client-core-2.6.0-cdh5.7.6.jar' \
        monicron.py ${1} --outdir="${OUT_DIR}" --config="${config}"
done
