#!/usr/bin/env python
# coding: utf-8

"""Retrieve ClassAds job information using Spark"""

from __future__ import print_function
from dateutil import parser as dparser
from subprocess import Popen, PIPE, STDOUT
from pyspark.sql import Column
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Column
import pyspark.sql.functions as fn
import pyspark.sql.types as types
import datetime as dt
import argparse
from utils import hadoop_paths, file_date
import schemas

# Configure pyspark
conf = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

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
                   col('data.CMSSite').alias('site'),
                   col('data.DESIRED_CMSDataset').alias('dataset'),
                   col('data.CRAB_Workflow').alias('workflow_id'),
                   col('data.CRAB_Id').alias('crab_id'),
                   col('data.CRAB_Retry').alias('retries'),
                   col('data.JobStartDate').alias('start_time'),
                   col('data.ScheddName').alias('schedd_name'),
                   col('data.CMSPrimaryPrimaryDataset').alias('primaryDataset_primary'),
                   col('data.CMSPrimaryProcessedDataset').alias('primaryDataset_processed'),
                   col('data.CMSPrimaryDataTier').alias('primaryDataTier'),
                   col('data.CMS_TaskType').alias('task'),
                   col('data.CRAB_UserHN').alias('user_hn'),
                   col('data.CoreHr').alias('walltime'),
                   col('data.CpuTimeHr').alias('cpuTime'),
                   col('data.ExitCode').alias('exitCode'),
                   col('data.RequestCpus').alias('cpus'),
                   col('data.ChirpCMSSWReadBytes').alias('read_bytes'),
                   col('data.ChirpCMSSWReadOps').alias('read_ops'),
                   col('data.ChirpCMSSWReadSegments').alias('read_segs'),
                   col('data.ChirpCMSSWReadTimeMsecs').alias('read_time'),
                   col('data.ChirpCMSSWReadVOps').alias('read_vops'),
                   col('data.ChirpCMSSWWriteBytes').alias('write_bytes'),
                   col('data.ChirpCMSSWWriteTimeMsecs').alias('write_time')
                   )
        )

    print("[script] Fetched {}".format(hdfs_path))
    return ds

def fetch(date_min, date_max, hdfs_base, hdfs_ext):
    """ Fetch ClassAds records between two given dates """
    # Get hdfs paths
    hdfs_paths = hadoop_paths(date_min, date_max, hdfs_base, hdfs_ext)
    # Get dataset
    ds = None
    for hdfs_path in hdfs_paths:
        _ds = fetch_xrootd(hdfs_path)
        if not ds:
            ds = _ds
        else:
            ds = ds.union(_ds)
        del _ds

    # Write to parquet file on hdfs
    fdate = file_date(date_min, date_max)
    (ds.write
       .option("compression","gzip")
       .mode("overwrite")
       .parquet("hdfs://analytix/user/jguiang/shared/ClassAds_"+fdate)
    )

    # Move from hdfs to eos
    p = Popen("hdfs dfs -get /user/jguiang/shared/ClassAds_"+fdate
              +" /eos/user/j/jguiang/data-access/parquet/",
              shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, 
              close_fds=True)

    print (p.stdout.read())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch HDFS ClassAds records.")
    parser.add_argument("--datemin", type=str, default=None, help="Minimum date")
    parser.add_argument("--datemax", type=str, default=None, help="Maximum date")
    args = parser.parse_args()

    now = dt.datetime.now()
    
    hdfs_base = "/project/monitoring/archive/xrootd/raw/gled"
    hdfs_ext = "json.gz"

    if args.datemin and args.datemax:
        date_max = dparser.parse(args.datemax)
        date_min = dparser.parse(args.datemin)
        if date_min > date_max:
            print("ERROR: given minimum date > maximum date")
        else:
            fetch(date_min, date_max, hdfs_base, hdfs_ext)
    else:
        yesterday = now - dt.timedelta(days=1)
        fetch(yesterday, now, hdfs_base, hdfs_ext)
