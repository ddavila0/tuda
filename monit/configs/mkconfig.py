import json

dummy_config = {
    "source_name": "test", 
    "hdfs_base": "/some/path/in/hdfs/",
    "hdfs_ext": "ext.gz"
}
 
with open("dummy.json", "w") as fout:
    json.dump(dummy_config, fout, indent=4, sort_keys=True)
