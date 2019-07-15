"""MONIT: MONIT Observes at Never-ending Intermittent Timestamps"""

import glob
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import timedelta, datetime as dt
from elastic import elastic
from hdfetchs import hdfetchs
from aggs import agg_utils

INTERVALS = ["daily", "weekly", "2 weeks", "4 weeks", "monthly",
             "6 months", "9 months", "yearly"]
SHORT_INTERVALS = INTERVALS[:5]
LONG_INTERVALS = INTERVALS[-3:]
PATH_TO_TOKEN = "./token"

def get_time_interval(interval):
    """Translate time interval string to datetimes"""
    now = dt.now()
    this_month = int(now.strftime("%m"))
    if interval == "daily":
        yesterday = now - timedelta(days=1)
        return yesterday, now
    elif interval == "weekly":
        last_week = now - timedelta(weeks=1)
        return last_week, now
    elif interval == "2 weeks":
        two_weeks_ago = now - timedelta(weeks=2)
        return two_weeks_ago, now
    elif interval == "4 weeks":
        four_weeks_ago = now - timedelta(weeks=4)
        return four_weeks_ago, now
    elif interval == "monthly":
        last_month = now.replace(
                         month=subtract_months(this_month, offset=1)
                     )
        return last_month, now
    elif interval == "6 months":
        six_months_ago = now.replace(
                             month=subtract_months(this_month, offset=6)
                         )
        return six_months_ago, now
    elif interval == "9 months":
        nine_months_ago = now.replace(
                              month=subtract_months(this_month, offset=9)
                          )
        return nine_months_ago, now
    elif interval == "yearly":
        last_year = now.replace(year=2018)
        return last_year, now
    else:
        raise ValueError("invalid interval")

def monit(interval_code):
    """Get relevant monitoring data for a given time interval"""
    results = {}
    interval_code = int(interval_code)
    if interval_code > len(INTERVALS)-1:
        raise ValueError("invalid interval code")
    else:
        interval = INTERVALS[interval_code]
        use_chunked_scan = INTERVALS[interval_code] in LONG_INTERVALS

        min_datetime, max_datetime = get_time_interval(interval)
        config_paths = glob.glob("./configs/*.json")

        for config_path in config_paths:
            with open(config_path, "r") as fin:
                config = json.load(fin)

            # Set up HDFS context
            hdfs = hdfetchs(min_datetime, max_datetime, 
                            config["hdfs_base"], config["hdfs_ext"],
                            config["source_name"])

            if use_chunked_scan:
                results[config["source_name"]] = hdfs.chunked_scan()

            else:
                results[config["source_name"]] = hdfs.direct_scan()

    return results

if __name__ == "__main__":
    argparser = ArgumentParser(description="Monitor cache health.",
                               formatter_class=RawTextHelpFormatter)
    interval_codes = ["{0} = {1}".format(c, i)
                      for c, i in enumerate(INTERVALS)]
    interval_help = ("Interval Codes:\n"+("\n").join(interval_codes))
    argparser.add_argument("-i", "--interval", type=int, default=1, 
                           help=interval_help)
    argparser.add_argument("-t", "--token", type=str, default="./token", 
                           help="Path to file with token")
    args = argparser.parse_args()

    PATH_TO_TOKEN = args.token
    monit(args.interval)
