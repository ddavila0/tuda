"""MONIT: MONIT Observes at Never-ending Intermittent Timestamps"""

import glob
import json
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import timedelta, datetime as dt
from hdfetchs import hdfetchs, get_file_date

INTERVALS = ["daily", "weekly", "2 weeks", "4 weeks", "monthly",
             "6 months", "9 months", "yearly"]
SHORT_INTERVALS = INTERVALS[:5]
LONG_INTERVALS = INTERVALS[-3:]

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
    argparser.add_argument("-i", "--interval", type=int, default=1, 
                           help=interval_help)
    args = argparser.parse_args()

    # Get MONIT results
    results = monit(args.interval)

    # Write MONIT results to json
    min_dt, max_dt = get_time_interval(INTERVALS[args.interval])
    f_date = get_file_date(min_dt, max_dt)
    with open("monit_{}.json".format(f_date), "w") as f_out:
        json.dump(results, f_out, indent=4, sort_keys=True)
