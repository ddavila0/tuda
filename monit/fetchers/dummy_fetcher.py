#!/usr/bin/env python
# coding: utf-8

from .fetch_utils import fetch_wrapper
from . import SPARK_SESSION as spark

@fetch_wrapper("test")
def dummy_fetch(path):
    """Print diagnostic information"""
    print("Spark session: {}".format(spark))
    print("Fetched",path)
    return None
