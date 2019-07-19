import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from .agg_utils import agg_wrapper

# ClassAds aggregations
@agg_wrapper(source_name="classads")
def num_unique_jobs(df):
    agg = df.select(
              fn.concat(
                  df.crab_id,
                  fn.lit("/"),
                  df.workflow_id,
                  fn.lit("_"),
                  df.num_retries.cast(StringType())
              ).alias("job_id"),
          ).agg(fn.countDistinct("job_id").alias("num_unique_jobs"))

    return agg.collect()[0]["num_unique_jobs"] 

@agg_wrapper(source_name="classads")
def num_unique_users(df):
    agg = df.agg(fn.countDistinct("user_hn").alias("num_unique_users"))
    return agg.collect()[0]["num_unique_users"]

@agg_wrapper(source_name="classads")
def total_walltime(df):
    agg = df.agg(fn.sum("walltime").alias("total_walltime"))
    return agg.collect()[0]["total_walltime"]

@agg_wrapper(source_name="classads")
def total_walltime_times_cpus(df):
    agg = df.agg(fn.sum(df.walltime*df.num_cpus)
                   .alias("total_walltime_times_cpus")
                )
    return agg.collect()[0]["total_walltime_times_cpus"]

@agg_wrapper(source_name="classads")
def total_cpu_time(df):
    agg = df.agg(fn.sum(df.cpu_time).alias("total_cpu_time"))
    return agg.collect()[0]["total_cpu_time"]

@agg_wrapper(source_name="classads")
def num_exit_code_zero(df):
    agg = df.agg(
              fn.count(
                  fn.when((df.exit_code == 0), True)
              ).alias("num_exit_code_zero")
          )
    return agg.collect()[0]["num_exit_code_zero"]

@agg_wrapper(source_name="classads")
def num_exits(df):
    return df.select("exit_code").count()

@agg_wrapper(source_name="classads", post_agg=True)
def cpu_eff(aggs):
    return aggs["total_cpu_time"]/aggs["total_walltime_times_cpus"]

@agg_wrapper(source_name="classads", post_agg=True)
def exit_code_frac(aggs):
    return aggs["num_exit_code_zero"]/aggs["num_exits"]
