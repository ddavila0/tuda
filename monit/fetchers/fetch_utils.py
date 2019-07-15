from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

FETCHERS = {}

class fetch_wrapper():
    """Wrapper for HDFetchS fetchers that maintains an object
       responsible for the organization of the fetcher system
    """

    def __init__(self, source_name):
        self.source_name = source_name

    def __call__(self, func):
        self.update_fetchers(func)

        def wrapped_func(*args, **kwargs):
            result = func(*args, **kwargs)
            return result

        return wrapped_func

    def update_fetchers(self, func):
        """Update global fetcher object"""
        global FETCHERS
        if self.source_name in FETCHERS:
            print("WARNING: {} already defined".format(self.source_name))
        else:
            FETCHERS[self.source_name] = func

        return

def get_fetcher(source_name):
    """Return fetcher for a given group"""
    global FETCHERS
    if not source_name in FETCHERS:
        valid_groups = (", ").join(FETCHERS.keys())
        raise ValueError("invalid group name "
                         + "(current groups: {})".format(valid_groups))

    return FETCHERS[source_name]
