import argparse
import datetime as dt
from dateutil import parser as dparser
from calendar import monthrange

def file_date(mindate_dt, maxdate_dt):
    mindate_str = mindate_dt.strftime("%m-%d-%Y")
    maxdate_str = maxdate_dt.strftime("%m-%d-%Y")
    file_date = ""

    if mindate_dt > maxdate_dt:
        print("ERROR: given minimum date > maximum date")
        return

    if mindate_dt and maxdate_dt:
        same_m = mindate_dt.strftime("%m") == maxdate_dt.strftime("%m")
        same_d = mindate_dt.strftime("%d") == maxdate_dt.strftime("%d")
        same_y = mindate_dt.strftime("%Y") == maxdate_dt.strftime("%Y")
        if same_m and same_d and same_y:
            file_date = mindate_str
        elif same_m and same_y:
            file_date = "{0}-{1}to{2}-{3}".format(mindate_dt.strftime("%m"),
                                                 mindate_dt.strftime("%d"),
                                                 maxdate_dt.strftime("%d"),
                                                 mindate_dt.strftime("%Y"))
        elif same_y:
            file_date = "{0}-{1}to{2}".format(mindate_dt.strftime("%m"),
                                             mindate_dt.strftime("%d"),
                                             maxdate_str)
        else:
            file_date = "{0}to{1}".format(mindate_str, maxdate_str)
            
    return file_date

def _day_range(day1, day2):
    days = range(day1, day2+1)
    regexes = {}
    for d in days:
        digits = [int(i) for i in str(d)]
        single = (len(digits) == 1)
        tens = "0" if single else str(digits[0])
        ones = str(digits[0] if single else digits[1])
        if tens in regexes:
            cur = regexes[tens]
            if ones not in cur:
                regexes[tens] = cur[:-1]+ones+"]"
        else:
            regexes[tens] = tens+"["+ones+"]"

    return regexes.values()

def _paths(day_min, day_max, month, year, hdfs_base, hdfs_ext):
    hdfs_paths = []
    dayrange = _day_range(day_min, day_max)
    month = str(month) if month >= 10 else "0"+str(month)
    for dr in dayrange:
        hdfs_paths.append("{0}/{1}/{2}/{3}/*.{4}".format(hdfs_base, year,
                                                         month, dr,
                                                         hdfs_ext))
    return hdfs_paths

def hadoop_paths(date_min, date_max, hdfs_base, hdfs_ext):
    # Require same year
    if date_min.year != date_max.year:
        print("ERROR: min/max dates must be in the same year")
        return

    hdfs_paths = []
    if date_min.month == date_max.month:
        hdfs_paths = _paths(date_min.day, date_max.day,
                                 date_min.month, date_min.year,
                                 hdfs_base, hdfs_ext)
    else:
        for month in range(date_min.month, date_max.month+1):
            month_start = (1 if month != date_min.month 
                           else date_min.day)
            month_end = (monthrange(date_min.year, month)[1]
                         if month != date_max.month
                         else date_max.day)
            month_paths = _paths(month_start, month_end, month,
                                 date_min.year, hdfs_base, hdfs_ext)
            hdfs_paths += month_paths

    return hdfs_paths

def _dummy_fetch(date_min, date_max, hdfs_base, hdfs_ext):
    hdfs_paths = hadoop_paths(date_min, date_max, hdfs_base, hdfs_ext)
    for path in sorted(hdfs_paths):
        print("Fetched",path)
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch HDFS XRootD records.")
    parser.add_argument("--datemin", type=str, default=None, help="Minimum date")
    parser.add_argument("--datemax", type=str, default=None, help="Maximum date")
    args = parser.parse_args()

    now = dt.datetime.now()
    
    hdfs_base = "/project/monitoring/archive/xrootd/raw/gled"
    hdfs_ext = "json.gz"

    if args.datemin and args.datemax:
        date_max = dparser.parse(args.datemax)
        date_min = dparser.parse(args.datemin)
        if date_min > date_max:
            print("ERROR: given minimum date > maximum date")
        else:
            _dummy_fetch(date_min, date_max, hdfs_base, hdfs_ext)
    else:
        yesterday = now - dt.timedelta(days=1)
        _dummy_fetch(yesterday, now, hdfs_base, hdfs_ext)
