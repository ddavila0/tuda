import glob
import json
import os
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import timedelta, datetime as dt
from calendar import monthrange
from hdfetchs import hdfetchs, get_file_date, get_all_file_paths

INTERVALS = ["weekly", "2 weeks", "4 weeks", "monthly",
             "2 months", "3 months", "4 months", "6 months", 
             "9 months", "yearly"]
SHORT_INTERVALS = INTERVALS[:5]
LONG_INTERVALS = list(set(INTERVALS)-set(SHORT_INTERVALS))

def subtract_months(this_month, offset=1):
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
        print(yesterday)
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

def run_over_yesterday(save=True):
    """Pull HDFS records for one day, store aggregations"""
    min_datetime, max_datetime = get_time_interval("yesterday")
    config_paths = list(set(glob.glob("./configs/*.json"))
                        - set(glob.glob("./configs/*.test.json")))
    # Pull HDFS records, run aggregations
    for config_path in config_paths:
        with open(config_path, "r") as f_in:
            config = json.load(f_in)

        # Set up HDFS context
        source_name = config["source_name"]
        hdfs = hdfetchs(min_datetime, max_datetime, 
                        config["hdfs_base"], config["hdfs_ext"],
                        source_name)

        results = hdfs.direct_scan()

        # Write results to JSON
        if save:
            user = os.environ["USER"]
            eos_base = "/eos/user/{0}/{1}/monitor/".format(user[0], user)
            date_dir = "{0}/{1}/".format(min_datetime.year, 
                                         min_datetime.month)
            out_dir = (eos_base
                       + "{0}/".format(source_name)
                       + "daily/"
                       + date_dir)
            if not os.path.exists(out_dir):
                os.mkdirs(out_dir)

            out_file = out_dir+"{0}.json".format(min_datetime.day)
            with open(out_file, "w") as f_out:
                json.dump(results, f_out)

    return results

def run_over_interval(interval, save=True, in_dir=""):
    """Pull daily aggregations within a given integral, store 
       aggregation over that interval
    """
    min_datetime, max_datetime = get_time_interval(interval)
    config_paths = list(set(glob.glob("./configs/*.json"))
                        - set(glob.glob("./configs/*.test.json")))

    user = os.environ["USER"]
    eos_base = "/eos/user/{0}/{1}/monitor/".format(user[0], user)


    for config_path in config_paths:
        with open(config_path, "r") as f_in:
            config = json.load(f_in)

        source_name = config["source_name"]
        base = "{0}/{1}/daily/".format(eos_base, source_name)
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
                        agg_utils.add_aggs(aggs, new_aggs)

        results = agg_utils.run_post_aggs(aggs, source_name)
        if save:
            out_dir = (eos_base
                       + "{0}/".format(source_name)
                       + "{0}/".format(interval)
                       + "{0}/".format(min_datetime.year))
            if not os.path.exists(out_dir):
                os.mkdirs(out_dir)

            file_date = get_file_date(min_datetime, max_datetime) 
            out_file = out_dir+file_date+".json"
            with open(out_file, "w") as f_out:
                json.dump(results, f_out)

    return results

def run_direct(interval_code):
    """Get relevant monitoring data for a given time interval"""
    results = {}
    if interval_code > len(INTERVALS)-1:
        raise ValueError("invalid interval code")
    else:
        interval = INTERVALS[interval_code]
        use_chunked_scan = interval in LONG_INTERVALS

        min_datetime, max_datetime = get_time_interval(interval)
        config_paths = list(set(glob.glob("./configs/*.json"))
                            - set(glob.glob("./configs/*.test.json")))

        for config_path in config_paths:
            with open(config_path, "r") as f_in:
                config = json.load(f_in)

            print(json.dumps(config, indent=4))

            # Set up HDFS context
            source_name = config["source_name"]
            hdfs = hdfetchs(min_datetime, max_datetime, 
                            config["hdfs_base"], config["hdfs_ext"],
                            source_name)

            if use_chunked_scan:
                results[source_name] = hdfs.chunked_scan()
            else:
                results[source_name] = hdfs.direct_scan()

    return results

if __name__ == "__main__":
    # CLI
    argparser = ArgumentParser(description="Monitor cache health.",
                               formatter_class=RawTextHelpFormatter)
    interval_codes = ["{0} = {1}".format(c, i)
                      for c, i in enumerate(INTERVALS)]
    interval_help = ("Interval Codes:\n"+("\n").join(interval_codes))
    argparser.add_argument("interval", type=int, nargs="?",
                           help=interval_help)
    args = argparser.parse_args()
    if args.interval:
        if args.interval > len(INTERVALS)-1:
            raise ValueError("invalid interval code")
        else:
            interval = INTERVALS[args.interval]
            print("computing aggs for "+interval)
            results = run_over_interval(interval)
            print(json.dumps(results, indent=4))
    else:
        print("computing aggs for yesterday:")
        results = run_over_yesterday()
        print(json.dumps(results, indent=4))
