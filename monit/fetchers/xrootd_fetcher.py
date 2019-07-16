#!/usr/bin/env python
# coding: utf-8

import os
from pyspark.sql.functions import col
from .fetch_utils import fetch_wrapper, SPARK_SESSION as spark
import schemas

@fetch_wrapper("xrootd")
def fetch_xrootd(hdfs_path):
    """Fetch HDFS XRootD records from a particular path"""
    # DBS file info
    csvreader = spark.read.format("com.databricks.spark.csv").option("nullValue","null").option("mode", "FAILFAST")
    dbs_base="/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current"
    dbs_files = csvreader.schema(schemas.schema_files()).load(dbs_base+"/FILES/part-m-00000")

    # Define servers that confirm cache is at UCSD or CalTech
    servers = ["xrd-cache-1", "xrd-cache-2", "xcache-00", "xcache-01",
               "xcache-02", "xcache-03", "xcache-04", "xcache-05", 
               "xcache-06", "xcache-07", "xcache-08", "xcache-09",
               "xcache-10"]
    # Get job reports
    jobreports = spark.read.json(hdfs_path)

    # Get dataset
    ds = (jobreports
            # Require that server is in defined set
            .filter(col('data.server_host').isin(servers))
            # Require only CMS jobs
            .filter(col('data.vo') == "cms")
            # Get the job id from DBS  
            .join(dbs_files, col('data.file_lfn')==col('f_logical_file_name'))
            # Select columns to save
            .select(
                   col('data.operation').alias('operation'),
                   col('data.app_info').alias('app_info'),
                   col('data.file_lfn').alias('file_name'),
                   col('f_file_size').alias('file_size'),
                   col('data.read_bytes').alias('read_bytes')
                   )
        )

    print("[script] Fetched {}".format(hdfs_path))
    return ds

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch HDFS XRootD records.")
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
            hdfetchs = HDFetchS(date_min, date_max, hdfs_base, hdfs_ext,
                                fetch_xrootd, out_name="XRootD")
            hdfetchs.fetch()
            hdfetchs.write()
    else:
        yesterday = now - dt.timedelta(days=1)
        hdfetchs = HDFetchS(yesterday, now, hdfs_base, hdfs_ext,
                            fetch_xrootd, out_name="XRootD")
        hdfetchs.fetch()
        hdfetchs.write()
