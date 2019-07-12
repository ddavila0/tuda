import glob

MODULE_DIR = __file__.split("__init__")[0]
MODULE_NAME = MODULE_DIR.split("/")[-2]

# Import agg modules to populate agg_utils.CONFIGS
excepts = [__file__, MODULE_DIR+"agg_utils.py"]
paths = list(set(glob.glob(MODULE_DIR+"*.py"))-set(excepts))
for path_to_agg in paths:
    agg_module = (path_to_agg.split("/")[-1]).split(".py")[0]
    __import__(MODULE_NAME+"."+agg_module)
