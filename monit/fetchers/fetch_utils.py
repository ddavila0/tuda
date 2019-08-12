from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Configure pyspark
SPARK_CONFIG = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
SPARK_CONTEXT = SparkContext(conf=SPARK_CONFIG)
SPARK_SESSION = SparkSession(SPARK_CONTEXT)

FETCHERS = {}

class fetch_wrapper():
    """Wrapper for HDFetchS fetchers that maintains an object
       responsible for the organization of the fetcher system
    """

    def __init__(self, tag, cache=""):
        self.tag = tag if not cache else cache+"_"+tag

    def __call__(self, func):
        self.update_fetchers(func)

        def wrapped_func(*args, **kwargs):
            result = func(*args, **kwargs)
            return result

        return wrapped_func

    def update_fetchers(self, func):
        """Update global fetcher object"""
        global FETCHERS
        if self.tag in FETCHERS:
            print("WARNING: {} already defined".format(self.tag))
        else:
            FETCHERS[self.tag] = func

        return

def get_fetcher(tag):
    """Return fetcher for a given source"""
    global FETCHERS
    if not tag in FETCHERS:
        valid_sources = (", ").join(FETCHERS.keys())
        raise ValueError("invalid source name "
                         + "(current source: {})".format(valid_sources))

    return FETCHERS[tag]
