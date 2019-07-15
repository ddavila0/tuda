import json

ex_config ={
    "source_name": "classads", 
    "hdfs_base": "/project/monitoring/archive/condor/raw/metric",
    "hdfs_ext": "json.gz"
}
 
with open("example.json", "w") as fout:
    json.dump(ex_config, fout, indent=4, sort_keys=True)
