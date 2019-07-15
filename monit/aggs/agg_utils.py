import json
import os
import glob

CONFIGS = {}

class agg_wrapper():
    """Wrapper for a MONIT aggregator that maintains a configuration
       object responsible for the organization of MONIT's plugin system
    """

    def __init__(self, source_name="aggs", post_agg=False):
        self.source_name = source_name
        self.agg_type = "post_aggs" if post_agg else "aggs"

    def __call__(self, func):
        self.update_config(func)

        def wrapped_func(*args, **kwargs):
            result = func(*args, **kwargs)
            return result

        return wrapped_func

    def update_config(self, func):
        """Update global config object"""
        global CONFIGS
        agg_type = self.agg_type
        if self.source_name in CONFIGS:
            config = CONFIGS[self.source_name]
            if agg_type in config and func not in config[agg_type]:
                config[agg_type].append(func)
            elif agg_type not in config:
                config[agg_type] = [func]
        else:
            config = {agg_type: [func]}
            CONFIGS[self.source_name] = config

        return

def run_aggs(df, source_name="aggs"):
    """Run aggregations from a given group"""
    global CONFIGS
    if not source_name in CONFIGS:
        valid_groups = (", ").join(CONFIGS.keys())
        raise ValueError("invalid group name "
                         + "(current groups: {})".format(valid_groups))
    else:
        results = {}
        config = CONFIGS[source_name]
        if not "aggs" in config:
            raise Exception("no aggregations in "+source_name)
        else:
            for func in config["aggs"]:
                results[func.__name__] = func(df)

    return results

def run_post_aggs(results, source_name="aggs"):
    """Run post-aggregation aggregations from a given group"""
    global CONFIGS
    if not source_name in CONFIGS:
        valid_groups = (", ").join(CONFIGS.keys())
        raise ValueError("invalid group name "
                         + "(current groups: {})".format(valid_groups))
    else:
        config = CONFIGS[source_name]
        if not "post_aggs" in config:
            print("WARNING: no post-aggs to run")
            return results
        else:
            for func in config["post_aggs"]:
                results[func.__name__] = func(results)

    return results
