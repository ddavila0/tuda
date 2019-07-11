import json

aggregators = []
with open("xrootd.json", "r") as fin:
    config = json.load(fin)
    for module, functions in config.items():
        print(module)
        print(functions)
        for f in functions:
            aggregators.append(getattr(__import__(module), f))

print(aggregators)
