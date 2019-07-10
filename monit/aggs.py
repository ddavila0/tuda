from datetime import datetime as dt
import pandas as pd
import numpy as np
import json

def agg_xrootd(df, do_division=True):
    """Make aggregations for a XRootD Dataframe"""
    # Dict to return
    aggs = {}
    numers = np.array([])
    denoms = np.array([])
    to_divide = []

    # Bookkeeping columns
    workflow_idFront = (df.app_info.str.split('/').str[-1]
                                   .str.split(':').str[:2]
                                   .str.join('_')
                       ) # Front half of workflow_id
    workflow_idBack = (df.app_info.str.split('/').str[-1]
                                  .str.split(':').str[2:]
                                  .str.join('_')
                      ) # Back half of workflow_id
    df["workflow_id"] = workflow_idFront.map(str)+":"+workflow_idBack
    df["crab_id"] = df.app_info.str.split('_').str[0]
    df["job_id"] = df.crab_id.map(str)+"/"+df.workflow_id
    df["start_datetime"] = pd.to_datetime(df.start_time, unit="ms")

    # General aggregations
    aggs["working_set"] = (df[df.operation == "read"]
                             .drop_duplicates(["file_name", "file_size"])
                             .file_size.sum()/1e12)
    aggs["total_naive_reads"] = df.file_size.sum()/1e12
    aggs["total_actual_reads"] = df.read_bytes.sum()/1e12
    # Reuse multiplier
    df_by_file = df.groupby("file_name")
    # Definition 1
    rmult_numer_1 = (df_by_file.app_info.nunique()).sum()
    rmult_denom_1 = df.file_name.nunique()
    aggs["rmult_1"] = rmult_numer_1/rmult_denom_1
    # Definition 2
    rmult_numer_2 = (df_by_file.app_info.nunique()
                     *df_by_file.file_size.apply(lambda g: g.unique()[0])
                    ).sum()
    rmult_denom_2 = np.sum(df["file_size"].unique())
    aggs["rmult_2"] = rmult_numer_2/rmult_denom_2
    # Definition 3
    rmult_numer_3 = (df_by_file.app_info.nunique()
                     *df_by_file.read_bytes.sum()
                    ).sum()
    rmult_denom_3 = rmult_denom_2
    aggs["rmult_3"] = rmult_numer_3/rmult_denom_3
    
    if not do_division:
        aggs["numers"] = list(numers)
        aggs["denoms"] = list(denoms)
    else:
        quotients = numers/denoms
        for i, q in enumerate(quotients):


    return aggs

def agg_classads(df):
    """Make aggregations for a ClassAds Dataframe"""
    # Dict to return
    aggs = {}

    # Bookkeeping columns
    df["workflow_id"] = df.workflow_id.map(str)+"_"+df.retries.astype(str)
    df["job_id"] = df.crab_id.map(str)+"/"+df.workflow_id
    df["start_datetime"] = pd.to_datetime(df.start_time, unit="ms")

    # General aggregations
    aggs["N_jobs"] = df.job_id.nunique()
    aggs["N_unique_users"] = df.user_hn.nunique()
    aggs["total_walltime"] = df.walltime.sum()
    aggs["total_cpu_time"] = df.cpuTime.sum()
    aggs["exit_code_frac"] = np.sum(df.exitCode == 0)/df.shape[0]
    # CPU efficiency
    cpu_eff_numer = (df.walltime*df.cpus).sum()
    cpu_eff_denom = (df.cpuTime).sum()
    aggs["cpu_eff"] = cpu_eff_numer/cpu_eff_denom

    return aggs

if __name__ == "__main__":
    from pyarrow import parquet as pq
    parquet_dir = "./parquet"
    # XRootD aggregations test
    dataset = pq.ParquetDataset(parquet_dir+"/XRootD_06-23to29-2019")
    table = dataset.read()
    df = table.to_pandas()
    aggs = aggs_xrootd(df)
    print("XRootD:")
    print(json.dumps(aggs))
    # ClassAds aggregations test
    dataset = pq.ParquetDataset(parquet_dir+"/ClassAds_06-23to29-2019")
    table = dataset.read()
    df = table.to_pandas()
    aggs = aggs_classads(df)
    print("ClassAds:")
    print(json.dumps(aggs))
