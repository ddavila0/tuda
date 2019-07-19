import pyspark.sql.functions as fn
from .agg_utils import agg_wrapper

# XRootD aggregations
@agg_wrapper(source_name="xrootd")
def working_set(df, chunked=False):
    agg = (df.where(df.operation == "read")
             .select(["file_size", "file_name"])
             .dropDuplicates()
             .agg(fn.sum("file_size").alias("working_set"))
          )
    return agg.collect()[0]["working_set"]/1e12

@agg_wrapper(source_name="xrootd")
def total_naive_reads(df):
    agg = df.agg(fn.sum("file_size").alias("total_naive_reads"))
    return agg.collect()[0]["total_naive_reads"]/1e12

@agg_wrapper(source_name="xrootd")
def total_actual_reads(df):
    agg = df.agg(fn.sum("read_bytes").alias("total_actual_reads"))
    return agg.collect()[0]["total_actual_reads"]/1e12

@agg_wrapper(source_name="xrootd")
def num_unique_file_accesses(df):
    agg = (df.groupBy("file_name")
             .agg(fn.countDistinct("app_info").alias("num_accesses"))
             .agg(fn.sum("num_accesses").alias("num_unique_file_accesses"))
          )
    return agg.collect()[0]["num_unique_file_accesses"]

@agg_wrapper(source_name="xrootd")
def num_unique_files(df):
    agg = df.agg(fn.countDistinct("file_name").alias("num_unique_files"))
    return agg.collect()[0]["num_unique_files"]

@agg_wrapper(source_name="xrootd", post_agg=True)
def reuse_mult_1(aggs):
    return aggs["num_unique_file_accesses"]/aggs["num_unique_files"]

@agg_wrapper(source_name="xrootd", post_agg=True)
def reuse_mult_2(aggs):
    return aggs["total_naive_reads"]/aggs["working_set"]

@agg_wrapper(source_name="xrootd", post_agg=True)
def reuse_mult_3(aggs):
    return aggs["total_actual_reads"]/aggs["working_set"]
