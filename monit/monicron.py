import glob
import json
import os
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import timedelta, datetime as dt
from calendar import monthrange
from hdfetchs import hdfetchs
from utils import get_number_string, get_all_file_paths, get_file_date
from aggs import agg_utils

INTERVALS = ["weekly", "2 weeks", "4 weeks", "monthly",
             "2 months", "3 months", "4 months", "6 months", 
             "9 months", "yearly"]
SHORT_INTERVALS = INTERVALS[:5]
LONG_INTERVALS = list(set(INTERVALS)-set(SHORT_INTERVALS))
USER = os.environ["USER"]
EOS_DIR = "/eos/user/{0}/{1}/monitor/".format(USER[0], USER)
        
def subtract_months(this_month, offset=1):
    """Step back some number of months as on a calendar"""
    if offset < 0 or offset > 11:
        raise ValueError("invalid month offset")
    result = this_month-offset
    if result < 0:
        return 12+result
    else:
        return result

def get_time_interval(interval):
    """Translate time interval string to datetimes"""
    now = dt.now()
    this_month = int(now.strftime("%m"))
    if interval == "yesterday":
        yesterday = now - timedelta(days=1, hours=1)
        start_of_yesterday = yesterday.replace(hour=0, minute=0, 
                                               second=0, microsecond=0)
        end_of_yesterday = yesterday.replace(hour=23, minute=59, 
                                             second=0, microsecond=0)
        return start_of_yesterday, end_of_yesterday
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
    elif interval == "2 months":
        two_months_ago = now.replace(
                             month=subtract_months(this_month, offset=2)
                         )
        return two_months_ago, now
    elif interval == "3 months":
        three_months_ago = now.replace(
                               month=subtract_months(this_month, offset=3)
                           )
        return three_months_ago, now
    elif interval == "4 months":
        four_months_ago = now.replace(
                              month=subtract_months(this_month, offset=4)
                           )
        return four_months_ago, now
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
        last_year = now.replace(year=now.year-1)
        return last_year, now
    else:
        raise ValueError("invalid interval")

def add_metadata(func):
    """Format the results (aggregations) of a Monicron function"""
    def wrapped(*args, **kwargs):
        config = args[0]
        # Run/time function
        start_time = dt.now()
        results = func(*args, **kwargs)
        end_time = dt.now()
        # Add information
        results["start_time"] = start_time
        results["end_time"] = end_time
        results["namespace"] = config["namespace"]
        results["cache_name"] = config["cache_name"]

        return results

    return wrapped

@add_metadata
def run_over_yesterday(config, save=True, out_base_dir=EOS_DIR):
    """Pull HDFS records for one day, store aggregations"""
    min_datetime, max_datetime = get_time_interval("yesterday")

    # Set up HDFS context
    hdfs = hdfetchs(min_datetime, max_datetime, 
                    config["hdfs_base"], config["hdfs_ext"], 
                    config["tag"])
    # Scan over HDFS files
    results = hdfs.direct_scan()

    return results

@add_metadata
def run_over_interval(config, interval, save=True, in_base_dir=EOS_DIR):
    """Pull daily aggregations within a given integral, store 
       aggregation over that interval
    """
    min_datetime, max_datetime = get_time_interval(interval)

    tag = config["tag"]
    in_path = (in_base_dir
            + "{0}/".format(source_name)
            + "daily/")
    to_glob = get_all_file_paths(base, "json", min_datetime, 
                                 max_datetime)
    aggs = {}
    for glob_pattern in to_glob:
        agg_json_paths = glob.glob(glob_pattern)
        for agg_json_path in agg_json_paths:
            with open(agg_json_path, "r") as agg_json:
                new_aggs = json.load(agg_json)
                if not aggs:
                    aggs = new_aggs
                else:
                    aggs = agg_utils.add_aggs(aggs, new_aggs)

    results = agg_utils.run_post_aggs(aggs, tag)

    return results

def credentials(file_path=""):
    """Read credentials from WMA_BROKER environment"""
    if not file_path:
        file_path = os.environ.get("WMA_BROKER", "")
    if not os.path.isfile(file_path):
        return {}
    with open(file_path, "r") as fin:
        creds = json.load(fin)

    return creds

def monicron(interval_code, config_path, creds_path, verbose=True)
    """Run aggregations and push results through StompAMQ service"""
    with open(config_path, "r") as fin:
        config = json.load(fin)

    if type(interval_code) == int:
        if interval_code > len(INTERVALS)-1:
            raise ValueError("invalid interval code")
        else:
            interval = INTERVALS[interval_code]
            if verbose: 
                print("[monicron] Computing aggs for "+interval)

            results = run_over_interval(config, interval, in_base_dir=out_base_dir)

            if verbose:
                print("[monicron] Results:")
                print(json.dumps(results, indent=4))
    else:
        if verbose:
            print("[monicron] computing aggs for yesterday:")

        results = run_over_yesterday(config)

        # Establish StompAMQ context
        creds = credentials(creds_file)
        host, port = creds['host_and_ports'].split(':')
        port = int(port)
        if  creds and StompAMQ:
            if verbose:
                print("[monicron] Sending results via StompAMQ")

            amq = StompAMQ(creds['username'], creds['password'], 
                           creds['producer'], creds['topic'], 
                           validation_schema=None, 
                           host_and_ports=[(host, port)])

            hid = results.get("hash", 1)
            notification, _, _ = amq.make_notification(results, hid)

            if verbose:
                print("[monicron] Delivered the following package:")
                print(json.dumps(notification, indent=4))

        else:
            raise ValueError("missing StompAMQ credentials file")

if __name__ == "__main__":
    # CLI
    argparser = ArgumentParser(description="Monitor cache health.",
                               formatter_class=RawTextHelpFormatter)
    # Interval
    interval_codes = ["{0} = {1}".format(c, i)
                      for c, i in enumerate(INTERVALS)]
    interval_help = ("Interval Codes:\n"+("\n").join(interval_codes))
    argparser.add_argument("interval", type=int, nargs="?",
                           help=interval_help)
    # Output directory
    argparser.add_argument("-o", "--outdir", type=str, default=EOS_DIR,
                           help="Full path to output directory")
    # Configuration file
    argparser.add_argument("--config", type=str, default=None,
                           help="Path to config .json file")
    args = argparser.parse_args()
    # StompAMQ credentials file
    argparser.add_argument("--creds", type=str, default=None,
                           help="Path to StompAMQ credentials .json file")
    args = argparser.parse_args()

    # Check args
    if not args.config:
        raise ValueError("invalid path to config file")
    if args.outdir[-1] != "/":
        args.outdir += "/"

    # Run Monicron
    monicron(args.interval, args.config, args.creds, args.outdir)
