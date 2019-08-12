import json
import glob

CONFIGS = {}

class agg_wrapper():
    """Wrapper for a MONIT aggregator that maintains a configuration
       object responsible for the organization of MONIT's plugin system
    """

    def __init__(self, tags=["aggs"], cache="", post_agg=False):
        self.tags = tags if not cache else [cache+"_"+t for t in tags]
        self.agg_type = "post_aggs" if post_agg else "aggs"

        if type(tags) != list:
            self.tags = [str(tags)]

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
        for tag in self.tags:
            if tag in CONFIGS:
                config = CONFIGS[tag]
                if agg_type in config and func not in config[agg_type]:
                    config[agg_type].append(func)
                elif agg_type not in config:
                    config[agg_type] = [func]
            else:
                config = {agg_type: [func]}
                CONFIGS[tag] = config

        return

def run_aggs(df, tag="aggs"):
    """Run aggregations from a given source"""
    global CONFIGS
    if not tag in CONFIGS:
        valid_sources = (", ").join(CONFIGS.keys())
        raise ValueError("invalid source name "
                         + "(current sources: {})".format(valid_sources))
    else:
        results = {}
        config = CONFIGS[tag]
        if not "aggs" in config:
            raise Exception("no aggregations in "+tag)
        else:
            for func in config["aggs"]:
                results[func.__name__] = func(df)

    return results

def run_post_aggs(results, tag="aggs"):
    """Run post-aggregation aggregations from a given source"""
    global CONFIGS
    if not tag in CONFIGS:
        valid_sources = (", ").join(CONFIGS.keys())
        raise ValueError("invalid source name "
                         + "(current sources: {})".format(valid_sources))
    else:
        config = CONFIGS[tag]
        if not "post_aggs" in config:
            print("WARNING: no post-aggs to run")
            return results
        else:
            for func in config["post_aggs"]:
                results[func.__name__] = func(results)

    return results

def add_aggs(current_aggs, aggs_to_add):
    """Add two aggregation results dictionaries together"""
    results = {a: current_aggs[a]+aggs_to_add[a] 
               for a in current_aggs.keys()}

    return results
