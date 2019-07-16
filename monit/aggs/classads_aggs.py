from .agg_utils import agg_wrapper
import numpy as np

# ClassAds aggregations
@agg_wrapper(source_name="classads")
def num_unique_jobs(df):
    df["workflow_id"] = (df.workflow_id.map(str)
                         + "_"
                         + df.num_retries.astype(str))
    df["job_id"] = (df.crab_id.map(str)
                    + "/"
                    + df.workflow_id)
    return df.job_id.nunique()

@agg_wrapper(source_name="classads")
def num_unique_users(df):
    return df.user_hn.nunique()

@agg_wrapper(source_name="classads")
def total_walltime(df):
    return df.walltime.sum()

@agg_wrapper(source_name="classads")
def total_walltime_times_cpus(df):
    return (df.walltime*df.num_cpus).sum()

@agg_wrapper(source_name="classads")
def total_cpu_time(df):
    return df.cpu_time.sum()

@agg_wrapper(source_name="classads")
def exit_code_frac(df):
    return np.sum(df.exit_code == 0)/df.shape[0]

@agg_wrapper(source_name="classads", post_agg=True)
def cpu_eff(aggs):
    return aggs["total_cpu_time"]/aggs["total_walltime_times_cpus"]
