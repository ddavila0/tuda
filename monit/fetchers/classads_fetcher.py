#!/usr/bin/env python
# coding: utf-8

"""Retrieve ClassAds job information using Spark"""

from pyspark.sql.functions import col
from .fetch_utils import fetch_wrapper
from . import SPARK_SESSION as spark
import schemas

@fetch_wrapper("classads")
def fetch_classads(hdfs_path):
    """Fetch HDFS ClassAds records from a particular path"""

    csvreader = spark.read.format("com.databricks.spark.csv").option("nullValue","null").option("mode", "FAILFAST")
    # Path where the input files are
    basepath="/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current"
    # Get the information about Blocks so that we can map the block name of the job to the block id 
    dbs_datasets = csvreader.schema(schemas.schema_datasets()).load(basepath+"/DATASETS")

    # Read input file
    jobreports = spark.read.json(hdfs_path)

    # The following regexps describe what is in the cache
    regexp1="/*/Run2016.*-03Feb2017.*/MINIAOD"
    regexp2="/*/RunIISummer16MiniAODv2-PUMoriond17_80X_.*/MINIAODSIM"
    regexp3="/*/.*-31Mar2018.*/MINIAOD"
    regexp4="/*/.*RunIIFall17MiniAODv2.*/MINIAODSIM"

    # Desired sites
    sites = ["T2_US_UCSD", "T2_US_Caltech", "T3_US_UCR"]

    ds = (jobreports
            # Joing dataset DBS table with jobreports
            .join(dbs_datasets, col('data.DESIRED_CMSDataset')==col('d_dataset'))
            # Require datasets from cache
            .filter(
                    col('d_dataset').rlike(regexp1) |
                    col('d_dataset').rlike(regexp2) | 
                    col('d_dataset').rlike(regexp3) | 
                    col('d_dataset').rlike(regexp4)
                   )
            # Require at UCSD, Caltech, or UCR
            .filter(col('data.CMSSite').isin(sites))
            # Require CMS jobs
            .filter(col('data.VO') == "cms")
            # Require analysis jobs
            .filter(col('data.Type') == 'analysis')
            # Require completed jobs
            .filter(col('data.Status') == 'Completed')
            # There are other utility CRAB jobs we don't want to read 
            .filter(col('data.JobUniverse') == 5)
            # Select columns to save
            .select(
                 col('data.CRAB_Workflow').alias('workflow_id'),
                 col('data.CRAB_Id').alias('crab_id'),
                 col('data.CRAB_Retry').alias('num_retries'),
                 col('data.ScheddName').alias('schedd_name'),
                 col('data.CRAB_UserHN').alias('user_hn'),
                 col('data.CoreHr').alias('walltime'),
                 col('data.CpuTimeHr').alias('cpu_time'),
                 col('data.ExitCode').alias('exit_code'),
                 col('data.RequestCpus').alias('num_cpus'),
                 col('data.ChirpCMSSWReadBytes').alias('read_bytes')
             )
        )

    return ds
