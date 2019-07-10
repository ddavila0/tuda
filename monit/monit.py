"""MONIT: MONIT Observes at Never-ending Intermittent Timestamps"""

from datetime import timedelta, datetime as dt
from argparse import ArgumentParser, RawTextHelpFormatter
# import aggs
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

def scan_months(month_min, month_max, year):
    total = 0
    for m in range(month_min, month_max+1):
        # Get month info
        month = "0"+str(m) if m < 10 else str(m)
        end = monthrange(year, m)[1]

        # Make datetime objects
        date_min = parser.parse("{0} 01 00:00:00 {1} UTC".format(month, year))
        date_max = parser.parse("{0} {1} 23:59:59 {2} UTC".format(month, end, year))

        # Fetch HDFS records
        hdfs = HDFetchS(date_min, date_max, hdfs_base, spark, fetch_xrootd)
        hdfs.fetch()
        if not hdfs.ds:
            to_print = "Got empty/null/NoneType pyspark dataset for {0} to {1}\n".format(date_min, date_max)
            print(to_print)
            with open("sideband_study.txt", "aw") as fout:
                fout.write(to_print)
            continue

        # Count unique jobs for each month
        hdfs.ds = hdfs.ds.filter(hdfs.ds.app_info != "")
        count = hdfs.ds.count()
#         df = hdfs.ds.toPandas()
#         if df.shape[0] == 0:
#             print("Got empty dataframe for {0} to {1}".format(date_min, date_max))
#             continue
#         total += np.sum(df.app_info != "")
        total += count

        # Print out info
        to_print = "{0} app info strings in df from {1} to {2}\n".format(count, date_min, date_max)
        print(to_print)
        with open("sideband_study.txt", "aw") as fout:
            fout.write(to_print)

        # Clean up
#         del df
    return total    

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
            if xrootd_df:
                xrootd_aggs = aggs.agg_xrootd(xrootd_df)
                del xrootd_df
            classads_df = elastic.fetch_classads(min_timestamp, 
                                                 max_timestamp)
            if classads_df:
                classads_aggs = aggs.agg_classads(classads_df)
                del classads_df
        elif INTERVALS[interval_code] in LONG_INTERVALS:
            # Use hadoop
            min_overall_date = dt.fromtimestamp(min_timestamp)
            max_overall_date = dt.fromtimestamp(max_timestamp)
            min_month = int(min_overall_date.strftime("%m"))
            max_month = int(max_overall_date.strftime("%m"))
            for m in range(min_month, max_month+1):
                xrootd_df = hdfetchs.fetch_xrootd(date_min, date_max)
                classads_df = hdfetchs.fetch_classads(date_min, date_max)
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
