from datetime import timedelta, datetime as dt
import os
from dateutil import parser
from calendar import monthrange
from fetchers import fetch_utils
from aggs import agg_utils

class hdfetchs():
    """HDFS Dynamic Fetch Script (HDFetchS)"""

    def __init__(self, min_datetime, max_datetime, hdfs_base, hdfs_ext,
                 source_name):
        self.min_datetime = min_datetime
        self.max_datetime = max_datetime
        self.hdfs_base = hdfs_base
        self.hdfs_ext = hdfs_ext
        self.fetcher = fetch_utils.get_fetcher(source_name)
        self.source_name = source_name

    def direct_scan(self):
        """Fetch and aggregate HDFS records within some time range less
           than a month
        """
        df = self.fetch()
        df = df.toPandas()
        results = agg_utils.run_aggs(df, self.source_name)
        results = agg_utils.run_post_aggs(results, self.source_name)

        return results

    def chunked_scan(self):
        """Fetch and aggregate HDFS records in month-long chunks"""
        results = {}
        # Save min/max datetime states locally
        min_month = self.min_datetime.month
        max_month = self.max_datetime.month
        min_day = self.min_datetime.day
        max_day = self.max_datetime.day
        if self.min_datetime.year != self.max_datetime.year:
            min_year = self.min_datetime.year
            max_year = self.max_datetime.year
            # Min to end of min year
            aggs_1 = self.scan_month_range(min_day, 31, min_month, 12,
                                           min_year)
            # Start of max year to max
            aggs_2 = self.scan_month_range(1, max_day, 1, max_month,
                                           max_year)
            results = agg_utils.add_aggs(aggs_1, aggs_2)
        else:
            year = self.min_datetime.year
            results = self.scan_month_range(min_day, max_day, min_month,
                                            max_month, year) 

        results = agg_utils.run_post_aggs(results, self.source_name)

        return results

    def scan_month_range(self, min_day, max_day, min_month, max_month,
                         year):
        """Fetch and aggregate monthly HDFS records from a range of 
           months within the same year
        """
        results = {}
        min_day = get_number_string(min_day)
        max_day = get_number_string(max_day)
        for m in range(min_month, max_month+1):
            # Get month info
            month = get_number_string(m)
            start = "01" if m != min_month else min_day
            end = monthrange(year, m)[1] if m != max_month else max_day

            # Make datetime objects
            min_str = "{0} {1} 00:00:00 {2} UTC".format(month, start,
                                                        year)
            min_datetime = parser.parse(min_str)
            max_str = "{0} {1} 23:59:59 {2} UTC".format(month, end, year)
            max_datetime = parser.parse(max_str)

            # Fetch HDFS records
            self.min_datetime = min_datetime
            self.max_datetime = max_datetime
            df = self.fetch()
            if not df:
                continue
            else:
                aggs = agg_utils.run_aggs(df, self.source_name)
                if not results:
                    results = aggs
                else:
                    results = agg_utils.add_aggs(results, aggs)
                
        return results

    def fetch(self, save=False, out_name="hdfetchs"):
        """Fetch HDFS records between two given dates as pyspark 
           DataFrame
        """
        # Get dataframe
        df = None
        file_paths = get_all_file_paths(self.hdfs_base, self.hdfs_ext,
                                        self.min_datetime, 
                                        self.max_datetime)
        for hdfs_path in sorted(file_paths):
            df_chunk = self.fetcher(hdfs_path)
            if not df:
                df = df_chunk
            else:
                df = df.union(df_chunk)

        if save:
            self.write(df, out_name=out_name)

        return df

    def write(self, df, out_name="hdfetchs"):
        """Write fetched pyspark dataframe to parquet files"""
        # Generate file name
        f_date = get_file_date(min_datetime, max_datetime)
        f_name = ("_").join([out_name, f_date])

        # Write to parquet file on hdfs
        (df.write.option("compression","gzip").mode("overwrite")
                 .parquet(f_name))

        # Move from hdfs to eos
        user = os.environ["USER"]
        hdfs_loc = "/user/{0}/shared/{1}".format(user, f_name)
        eos_dest = "/eos/user/{0}/{1}/{2}".format(user[0], user, f_name)
        if os.path.exists(eos_dest):
            print("WARNING: file {} already exists".format(eos_dest))

        p = Popen("hdfs dfs -get {0} {1}".format(hdfs_loc, eos_dest),
                  shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, 
                  close_fds=True)

        return

def get_number_string(num):
    return "0"+str(num) if num < 10 else str(num)

def get_all_file_paths(base, ext, min_datetime, max_datetime):
    """Create full list of hadoop-like paths for a selection of files"""
    if base[-1] != "/":
        base += "/"
    if ext[0] == ".":
        ext = ext[1:]

    file_paths = []
    if min_datetime.year == max_datetime.year:
        year = min_datetime.year
        if min_datetime.month == max_datetime.month:
            month = min_datetime.month
            if min_datetime.day == max_datetime.day:
                month_str = get_number_string(month)
                day_str = get_number_string(min_datetime.day)
                path = "{0}/{1}/{2}/*.{3}".format(year, 
                                                  month_str, 
                                                  day_str, ext)
                file_paths = [base+path]
            else:
                file_paths = get_file_paths(base, ext,
                                            min_datetime.day,
                                            max_datetime.day,
                                            month, year)
        else:
            for month in range(min_datetime.month, max_datetime.month+1):
                min_day = (1 if month != min_datetime.month 
                           else min_datetime.day)
                max_day = (monthrange(min_datetime.year, month)[1]
                           if month != max_datetime.month
                           else max_datetime.day)
                file_paths += get_file_paths(base, ext, min_day, max_day, 
                                             month, year)
    else:
        end_of_min_year = "Dec 31 23:59:59 {}".format(min_datetime.year)
        file_paths += get_all_file_paths(base, ext, min_datetime, 
                                         parser.parse(end_of_min_year))
        beg_of_max_year = "Jan 01 00:00:00 {}".format(max_datetime.year)
        file_paths += get_all_file_paths(base, ext, 
                                         parser.parse(beg_of_max_year), 
                                         max_datetime)

    return file_paths

def get_file_paths(base, ext, min_day, max_day, month, year):
    """Create list of hadoop-like paths for a range of days in a month"""
    if min_day > max_day:
        raise ValueError("Given minimum day > maximum day")
    
    paths = []
    day_ranges = get_day_ranges(min_day, max_day)
    month = get_number_string(month)
    for day_range in day_ranges:
        path = "{0}/{1}/{2}/*.{3}".format(base, year, month, 
                                          day_range, ext)
        paths.append(base+path)

    return paths

def get_day_ranges(day1, day2):
    """Create list of regex expression to cover a range of days"""
    if day1 > day2:
        raise ValueError("Given first day > last day")

    days = range(day1, day2+1)
    regexes = {}
    for day in days:
        day_str = get_number_string(day)
        tens, ones = day_str[0], day_str[1]
        if tens in regexes:
            cur = regexes[tens]
            if ones not in cur:
                regexes[tens] = cur[:-1]+ones+"]"
        else:
            regexes[tens] = tens+"["+ones+"]"

    return regexes.values()

def get_file_date(min_datetime, max_datetime):
    """Create a unique string for naming output files"""
    if min_datetime > max_datetime:
        raise ValueError("given minimum date > maximum date")

    datestr_min = min_datetime.strftime("%m-%d-%Y")
    datestr_max = max_datetime.strftime("%m-%d-%Y")
    file_date = ""

    if min_datetime and max_datetime:
        same_m = min_datetime.month == max_datetime.month
        same_d = min_datetime.day == max_datetime.day
        same_y = min_datetime.year == max_datetime.year
        if same_m and same_d and same_y:
            file_date = datestr_min
        elif same_m and same_y:
            file_date = "{0}-{1}to{2}-{3}".format(min_datetime.strftime("%m"),
                                                  min_datetime.strftime("%d"),
                                                  max_datetime.strftime("%d"),
                                                  min_datetime.strftime("%Y"))
        elif same_y:
            file_date = "{0}-{1}to{2}".format(min_datetime.strftime("%m"),
                                              min_datetime.strftime("%d"),
                                              datestr_max)
        else:
            file_date = "{0}to{1}".format(datestr_min, datestr_max)
            
    return file_date

if __name__ == "__main__":
    from argparse import ArgumentParser
    import json

    argparser = ArgumentParser(description="Fetch HDFS records.")
    argparser.add_argument("--datemin", type=str, default=None,
                           help="Minimum date")
    argparser.add_argument("--datemax", type=str, default=None, 
                           help="Maximum date")
    argparser.add_argument("--config", type=str, default=None, 
                           help="Location of config file")
    args = argparser.parse_args()

    now = dt.now()
    with open(args.config, "r") as f_in:
        config = json.load(f_in)

    if args.datemin and args.datemax:
        max_datetime = parser.parse(args.datemax)
        min_datetime = parser.parse(args.datemin)

        hdfs = hdfetchs(min_datetime, max_datetime, config["hdfs_base"], 
                        config["hdfs_ext"], config["source_name"])
        df = hdfs.fetch()
    else:
        yesterday = now - dt.timedelta(days=1)
        hdfs = hdfetchs(yesterday, now, config["hdfs_base"], 
                        config["hdfs_ext"], config["source_name"])
        df = hdfs.fetch()
