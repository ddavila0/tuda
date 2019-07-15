import glob
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

MODULE_DIR = __file__.split("__init__")[0]
MODULE_NAME = MODULE_DIR.split("/")[-2]

# Import fetcher modules to populate fetch_utils.FETCHERS
excepts = [__file__, MODULE_DIR+"fetch_utils.py"]
paths = list(set(glob.glob(MODULE_DIR+"*.py"))-set(excepts))
for path_to_agg in paths:
    agg_module = (path_to_agg.split("/")[-1]).split(".py")[0]
    __import__(MODULE_NAME+"."+agg_module)

# Configure pyspark
SPARK_CONFIG = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
SPARK_CONTEXT = SparkContext(conf=SPARK_CONFIG)
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
