import glob

MODULE_DIR = __file__.split("__init__")[0]
MODULE_NAME = MODULE_DIR.split("/")[-2]

# Import fetcher modules to populate fetch_utils.FETCHERS
excepts = [__file__, MODULE_DIR+"fetch_utils.py"]
paths = list(set(glob.glob(MODULE_DIR+"*.py"))-set(excepts))
for path_to_fetcher in paths:
    fetch_module = (path_to_fetcher.split("/")[-1]).split(".py")[0]
    __import__(MODULE_NAME+"."+fetch_module)
