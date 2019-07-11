from aggregator import agg_wrapper

# XRootD aggregations
@agg_wrapper(__file__, group_name="xrootd")
def working_set(df, chunked=False):
    return (df[df.operation == "read"]
                .drop_duplicates(["file_name", "file_size"])
                .file_size.sum()/1e12)

@agg_wrapper(__file__, group_name="xrootd")
def total_naive_reads(df, chunked=False):
    return df.file_size.sum()/1e12

@agg_wrapper(__file__, group_name="xrootd")
def total_actual_reads(df, chunked=False):
    return df.read_bytes.sum()/1e12

# ClassAds aggregations
@agg_wrapper(__file__, group_name="classads")
def num_unique_jobs(df, chunked=False):
    return df.job_id.nunique()

@agg_wrapper(__file__, group_name="classads")
def num_unique_users(df, chunked=False):
    return df.user_hn.nunique()

@agg_wrapper(__file__, group_name="classads")
def total_walltime(df, chunked=False):
    return df.walltime.sum()

@agg_wrapper(__file__, group_name="classads")
def total_cpu_time(df, chunked=False):
    return df.cpu_time.sum()

@agg_wrapper(__file__, group_name="classads")
def exit_code_frac(df, chunked=False):
    return np.sum(df.exitCode == 0)/df.shape[0]
