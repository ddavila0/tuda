#!/usr/bin/env python
# coding: utf-8

import os
from .fetch_utils import fetch_wrapper, SPARK_SESSION as spark

@fetch_wrapper("test")
def dummy_fetch(path):
    """Print diagnostic information"""
    # Configure pyspark
    print("Spark session: {}".format(spark))
    print("Path to fetch: {}".format(path))
    return None
