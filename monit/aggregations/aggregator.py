import json
import os

class agg_wrapper():
    """Wrapper for a MONIT aggregator that maintains a configuration file
       essential to the organization of MONIT's plugin system
    """

    def __init__(self, module_file, group_name=None):
        self.module_name = os.path.basename(module_file).split(".py")[0]
        self.func_name = None
        self.group_name = group_name

    def __call__(self, func):
        self.func_name = func.__name__
        if self.group_name: self.group_config()

        def wrapped_func(*args, **kwargs):
            print("inside wrapper")
            print(args)
            print(kwargs)
            result = func(*args, **kwargs)
            print("wrapper done")
            return result

        return wrapped_func

    def group_config(self):
        """Update config file"""
        config_file = self.group_name+".json"
        if os.path.isfile(config_file):
            with open(config_file, "r+") as json_io:
                config = json.load(json_io)
                if (self.module_name in config and
                    self.func_name not in config[self.module_name]):
                        config[self.module_name].append(self.func_name)
                elif self.module_name not in config:
                    config[self.module_name] = [self.func_name]
                json_io.seek(0)
                json.dump(config, json_io)
                json_io.truncate()
        else:
            with open(config_file, "w") as json_out:
                group_config = {self.module_name: [self.func_name]}
                json.dump(group_config, json_out)
        return

