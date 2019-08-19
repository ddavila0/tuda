import glob
import json
import time
import os
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import timedelta, datetime as dt
from calendar import monthrange
from CMSMonitoring.StompAMQ import StompAMQ
from hdfetchs import hdfetchs
from utils import get_number_string, get_all_file_paths, get_file_date
from aggs import agg_utils

INTERVALS = ["weekly", "biweekly", "quadweekly", "monthly",
             "bimonthly", "trimonthly", "quadmonthly", "bianually", 
             "dodranially", "anually"]
        
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
    if interval == "daily":
        yesterday = now - timedelta(days=1, hours=1)
        start_of_yesterday = yesterday.replace(hour=0, minute=0, 
                                               second=0, microsecond=0)
        end_of_yesterday = yesterday.replace(hour=23, minute=59, 
                                             second=0, microsecond=0)
        return start_of_yesterday, end_of_yesterday
    elif interval == "weekly":
        last_week = now - timedelta(weeks=1)
        return last_week, now
    elif interval == "biweekly":
        two_weeks_ago = now - timedelta(weeks=2)
        return two_weeks_ago, now
    elif interval == "quadweekly":
        four_weeks_ago = now - timedelta(weeks=4)
        return four_weeks_ago, now
    elif interval == "monthly":
        last_month = now.replace(
                         month=subtract_months(this_month, offset=1)
                     )
        return last_month, now
    elif interval == "bimonthly":
        two_months_ago = now.replace(
                             month=subtract_months(this_month, offset=2)
                         )
        return two_months_ago, now
    elif interval == "trimonthly":
        three_months_ago = now.replace(
                               month=subtract_months(this_month, offset=3)
                           )
        return three_months_ago, now
    elif interval == "quadmonthly":
        four_months_ago = now.replace(
                              month=subtract_months(this_month, offset=4)
                           )
        return four_months_ago, now
    elif interval == "bianually":
        six_months_ago = now.replace(
                             month=subtract_months(this_month, offset=6)
                         )
        return six_months_ago, now
    elif interval == "dodranially":
        nine_months_ago = now.replace(
                              month=subtract_months(this_month, offset=9)
                          )
        return nine_months_ago, now
    elif interval == "anually":
        last_year = now.replace(year=now.year-1)
        return last_year, now
    else:
        raise ValueError("invalid interval")

def add_metadata(func):
    """Format the results (aggregations) of a Monicron function"""
    def wrapped(*args, **kwargs):
        config = args[0]
        # Run function
        results = func(*args, **kwargs)
        # Add information
        results["namespace"] = config["namespace"]
        results["cache_name"] = config["cache_name"]
        final_results = {
            "data": results,
            "metadata": {
                "type": config["source_name"],
                "topic": config["topic"]
            }
        }

        return final_results

    return wrapped

@add_metadata
def run_over_yesterday(config):
    """Pull HDFS records for one day, store aggregations"""
    min_datetime, max_datetime = get_time_interval("daily")

    # Set up HDFS context
    hdfs = hdfetchs(min_datetime, max_datetime, 
                    config["hdfs_base"], config["hdfs_ext"], 
                    config["tag"], cache_name=config["cache_name"])
    # Scan over HDFS files
    results = hdfs.direct_scan()
    results["start_time"] = int(time.mktime(min_datetime.timetuple()))
    results["end_time"] = int(time.mktime(max_datetime.timetuple()))

    return results

@add_metadata
def run_over_interval(config, interval, out_base_dir):
    """Pull daily aggregations within a given integral, store 
       aggregation over that interval
    """
    min_datetime, max_datetime = get_time_interval(interval)

    monicron_dir = "{0}/{1}/{2}/daily/".format(config["cache_name"], 
                                               config["source_name"],
                                               config["namespace"])
    base_dir = out_base_dir+monicron_dir
    to_glob = get_all_file_paths(base_path, "json", min_datetime, 
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

    results = agg_utils.run_post_aggs(aggs, config["tag"])
    results["start_time"] = int(time.mktime(min_datetime.timetuple()))
    results["end_time"] = int(time.mktime(max_datetime.timetuple()))

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

def monicron(interval_code, config_path, out_base_dir, creds_path):
    """Run aggregations and push results through StompAMQ service"""
    with open(config_path, "r") as fin:
        config = json.load(fin)
    if not out_base_dir[-1] == "/":
        out_base_dir += "/"

    # Get credentials
    creds = credentials(creds_path)
    host, port = creds["host_and_ports"].split(":")
    port = int(port)
    if not creds or not StompAMQ:
        raise ValueError("missing StompAMQ credentials file")

    # Establish StompAMQ context
    amq = StompAMQ(creds["username"], creds["password"], 
                   creds["producer"], config["topic"], 
                   validation_schema=None, 
                   host_and_ports=[(host, port)])

    interval = ""
    results = {}
    if type(interval_code) == int:
        if interval_code > len(INTERVALS)-1:
            raise ValueError("invalid interval code")
        else:
            interval = INTERVALS[interval_code]
            print("[monicron] Computing aggs for "+interval)
            # Run aggregations
            results = run_over_interval(config, interval, out_base_dir)
    else:
        interval = "daily"
        print("[monicron] computing aggs for yesterday:")
        # Run aggregations
        results = run_over_yesterday(config)

    # Format results and send notification through StompAMQ
    if not results:
        print("[monicron] WARNING: no results produced.")
        return
    else:
        # Get StompAMQ notification
        print("[monicron] Results:")
        print(json.dumps(results, indent=4))
        payload = results["data"]
        metadata = results["metadata"]
        hid = results.get("hash", 1)
        notification, _, _ = amq.make_notification(payload, hid,
                                                   metadata=metadata)

        # Get destination path in local cache
        min_datetime, max_datetime = get_time_interval(interval)
        monicron_dir = "{0}/{1}/{2}/{3}/".format(config["cache_name"], 
                                                 config["source_name"],
                                                 config["namespace"],
                                                 interval)
        date_dir = ""
        file_date = get_file_date(min_datetime, max_datetime)
        if interval == "daily":
            date_dir = "/".join(file_date.split("-"))+"/"
        else:
            date_dir = file_date+"/"

        out_path = out_base_dir+monicron_dir+date_dir
        if not os.path.exists(out_path):
            os.makedirs(out_path)

        # Write to local cache
        data = notification["body"]["data"]
        metadata = notification["body"]["metadata"]
        with open(out_path+"data.json", "w") as f_out:
            json.dump(data, f_out)
        with open(out_path+"metadata.json", "w") as f_out:
            json.dump(metadata, f_out)
        print("[monicron] Dumped results to {}".format(out_path))

        # Deliver package
        print("[monicron] Delivered the following package:")
        print(json.dumps(notification, indent=4))
        response = amq.send(data)
        print("[monicron] Response from AMQ:")
        print(json.dumps(response, indent=4))

    return

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
    argparser.add_argument("-o", "--outdir", type=str, default=None,
                           help="Full path to output directory")
    # Configuration file
    argparser.add_argument("--config", type=str, default=None,
                           help="Path to config file")
    # StompAMQ credentials file
    argparser.add_argument("--creds", type=str, default=None,
                           help="Path to StompAMQ credentials file")
    args = argparser.parse_args()

    # Check args
    if not os.path.exists(args.config):
        raise ValueError("invalid path to config file")

    # Run Monicron
    monicron(args.interval, args.config, args.outdir, args.creds)
