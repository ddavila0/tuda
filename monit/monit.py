"""MONIT: MONIT Observes at Never-ending Intermittent Timestamps"""

from datetime import timedelta, datetime as dt
from argparse import ArgumentParser, RawTextHelpFormatter
from calendar import monthrange
from aggregations import agg_utils
# import elastic
# import hdfetchs

INTERVALS = ["daily", "weekly", "2 weeks", "4 weeks", "monthly",
             "6 months", "9 months", "yearly"]
SHORT_INTERVALS = INTERVALS[:5]
LONG_INTERVALS = INTERVALS[-3:]

def get_timestamp(dt_obj):
    return float(dt_obj.strftime("%s.%f"))

def subtract_months(this_month, offset=1):
    if offset < 0 or offset > 11:
        raise ValueError("invalid month offset")
    result = this_month-offset
    if result < 0:
        return 12+result
    else:
        return result

def get_time_interval(interval):
    now = dt.now()
    this_month = int(now.strftime("%m"))
    if interval == "daily":
        yesterday = now - timedelta(days=1)
        return get_timestamp(yesterday), get_timestamp(now)
    elif interval == "weekly":
        last_week = now - timedelta(weeks=1)
        return get_timestamp(last_week), get_timestamp(now)
    elif interval == "2 weeks":
        two_weeks_ago = now - timedelta(weeks=2)
        return get_timestamp(two_weeks_ago), get_timestamp(now)
    elif interval == "4 weeks":
        four_weeks_ago = now - timedelta(weeks=4)
        return get_timestamp(four_weeks_ago), get_timestamp(now)
    elif interval == "monthly":
        last_month = now.replace(
                         month=subtract_months(this_month, offset=1)
                     )
        return get_timestamp(last_month), get_timestamp(now)
    elif interval == "6 months":
        six_months_ago = now.replace(
                             month=subtract_months(this_month, offset=6)
                         )
        return get_timestamp(six_months_ago), get_timestamp(now)
    elif interval == "9 months":
        nine_months_ago = now.replace(
                              month=subtract_months(this_month, offset=9)
                          )
        return get_timestamp(nine_months_ago), get_timestamp(now)
    elif interval == "yearly":
        last_year = now.replace(year=2018)
        return get_timestamp(last_year), get_timestamp(now)
    else:
        raise ValueError("invalid interval")

def scan_month_range(min_month, max_month, year):
    result = 0
    # TODO: Write this function!
    return result    

def scan_by_month(min_datetime, max_datetime, aggs):
    # TODO: properly add aggregations
    result = 0
    if min_datetime.year != max_datetime.year:
        result = scan_month_range(min_datetime.month, 12,
                                  min_datetime.year)
        result += scan_month_range(1, max_datetime.month,
                                   max_datetime.year)
    else:
        result = scan_month_range(min_datetime.month, max_datetime.month,
                                  min_datetime.year) 
    return result

def monit(interval_code):
    interval_code = int(interval_code)
    if interval_code > len(INTERVALS)-1:
        raise ValueError("invalid interval code")
    else:
        interval = INTERVALS[interval_code]
        min_timestamp, max_timestamp = get_time_interval(interval)
        if INTERVALS[interval_code] in SHORT_INTERVALS:
            # Use elastic search
            xrootd_df = elastic.fetch_xrootd(min_timestamp, 
                                             max_timestamp)
            xrootd_aggs = agg_utils.run_aggs(xrootd_df, "xrootd")
            del xrootd_df
            classads_df = elastic.fetch_classads(min_timestamp, 
                                                 max_timestamp)
            classads_aggs = agg_utils.run_aggs(classads_df, "classads")
            del classads_df
        elif INTERVALS[interval_code] in LONG_INTERVALS:
            # Use hadoop
            min_datetime = dt.fromtimestamp(min_timestamp)
            max_datetime = dt.fromtimestamp(max_timestamp)
            classads_aggs = scan_by_month(min_datetime, max_datetime,
                                          aggs.agg_classads)
            xrootd_aggs = scan_by_month(min_datetime, max_datetime,
                                        aggs.agg_xrootd)
    return

if __name__ == "__main__":
    parser = ArgumentParser(description="Monitor cache health.",
                            formatter_class=RawTextHelpFormatter)
    readable_interval_codes = ["{0} = {1}".format(c, i)
                               for c, i in enumerate(INTERVALS)]
    interval_help = ("Interval Codes:\n"
                     +("\n").join(readable_interval_codes))
    parser.add_argument("-i", "--interval", type=int, default=1, 
                        help=interval_help)
    args = parser.parse_args()
    monit(args.interval)
