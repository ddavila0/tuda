from .agg_utils import agg_wrapper

@agg_wrapper(source_names=["xrootd/mc", "xrootd/data", "xrootd/user"])
def working_set(df, chunked=False):
    return (df[df.operation == "read"]
                .drop_duplicates(["file_name", "file_size"])
                .file_size.sum())/1e12

@agg_wrapper(source_names=["xrootd/mc", "xrootd/data", "xrootd/user"])
def total_naive_reads(df):
    return df.file_size.sum()/1e12

@agg_wrapper(source_names=["xrootd/mc", "xrootd/data", "xrootd/user"])
def total_actual_reads(df):
    return df.read_bytes.sum()/1e12

@agg_wrapper(source_names=["xrootd/mc", "xrootd/data", "xrootd/user"])
def num_unique_file_accesses(df):
    return (df.groupby("file_name").app_info.nunique()).sum()

@agg_wrapper(source_names=["xrootd/mc", "xrootd/data", "xrootd/user"])
def num_unique_files(df):
    return df.file_name.nunique()

@agg_wrapper(source_names=["xrootd/mc", "xrootd/data", "xrootd/user"], 
             post_agg=True)
def reuse_mult_1(aggs):
    numer = float(aggs["num_unique_file_accesses"])
    denom = float(aggs["num_unique_files"])
    return 0 if denom == 0 else numer/denom

@agg_wrapper(source_names=["xrootd/mc", "xrootd/data", "xrootd/user"], 
             post_agg=True)
def reuse_mult_2(aggs):
    numer = aggs["total_naive_reads"]
    denom = aggs["working_set"]
    return 0 if denom == 0 else numer/denom

@agg_wrapper(source_names=["xrootd/mc", "xrootd/data", "xrootd/user"], 
             post_agg=True)
def reuse_mult_3(aggs):
    numer = aggs["total_actual_reads"]
    denom = aggs["working_set"]
    return 0 if denom == 0 else numer/denom
