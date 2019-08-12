from .agg_utils import agg_wrapper

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"])
def num_unique_jobs(df):
    df["workflow_id"] = (df.workflow_id.map(str)
                         + "_"
                         + df.num_retries.astype(str))
    df["job_id"] = (df.crab_id.map(str)
                    + "/"
                    + df.workflow_id)
    return df.job_id.nunique()

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"])
def num_unique_users(df):
    return df.user_hn.nunique()

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"])
def total_walltime(df):
    return df.walltime.sum()

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"])
def total_walltime_times_cpus(df):
    return (df.walltime*df.num_cpus).sum()

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"])
def total_cpu_time(df):
    return df.cpu_time.sum()

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"])
def num_exit_code_zero(df):
    return (df.exit_code == 0).sum()

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"])
def num_exits(df):
    return df.shape[0]

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"], 
             post_agg=True)
def exit_code_frac(aggs):
    numer = float(aggs["num_exit_code_zero"])
    denom = float(aggs["num_exits"])
    return 0 if denom == 0 else numer

@agg_wrapper(tags=["classads_miniaod", "classads_nanoaod"],
             post_agg=True)
def cpu_eff(aggs):
    numer = aggs["total_cpu_time"]
    denom = aggs["total_walltime_times_cpus"]
    return 0 if denom == 0 else numer
