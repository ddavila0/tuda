import json
import os
import glob
from aggregations import MODULE_DIR, MODULE_NAME

CONFIGS = {}

class agg_wrapper():
    """Wrapper for a MONIT aggregator that maintains a configuration file
       essential to the organization of MONIT's plugin system
    """

    def __init__(self, agg_file_dunder, group_name="aggs"):
        self.func_name = None
        self.group_name = group_name

        # Prepend module name to agg file name
        self.agg_module_name = (agg_file_dunder.split("/")[-1]
                                               .split(".py")[0])

    def __call__(self, func):
        self.func_name = func.__name__
        self.update_config()

        def wrapped_func(*args, **kwargs):
            print("inside wrapper")
            print(args)
            print(kwargs)
            result = func(*args, **kwargs)
            print("wrapper done")
            return result

        return wrapped_func

    def update_config(self):
        """Update global config object"""
        global CONFIGS
        if self.group_name in CONFIGS:
            config = CONFIGS[self.group_name]
            # Update config
            if (self.agg_module_name in config and
                self.func_name not in config[self.agg_module_name]):
                    config[self.agg_module_name].append(self.func_name)
            elif self.agg_module_name not in config:
                config[self.agg_module_name] = [self.func_name]
        else:
            config = {self.agg_module_name: [self.func_name]}
            CONFIGS[self.group_name] = config
        return

def run_aggs(df, group_name):
    """Run aggregations from a given group"""
    global CONFIGS
    if not group_name in CONFIGS:
        valid_groups = (", ").join(CONFIGS.keys())
        raise ValueError("invalid group name "
                         +"(current groups: {})".format(valid_groups))
    else:
        result = {}
        config = CONFIGS[group_name]
        for module, func_names in config.items():
            agg_module = getattr(__import__(MODULE_NAME), module)
            for func_name in func_names:
                agg = getattr(agg_module, func_name)
                result[func_name] = agg(df)

    return result
