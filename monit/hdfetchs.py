import datetime as dt
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

        if hdfs_base[-1] == "/":
            self.hdfs_base = hdfs_base[:-1]
        if hdfs_ext[0] == ".":
            self.hdfs_ext = hdfs_ext[1:]
        return

    def direct_scan(self):
        """Fetch and aggregate HDFS records within some time range less than
           a month
        """
        ds = self.fetch()
        df = ds.toPandas()
        aggs = agg_utils.run_aggs(df, self.source_name)
        aggs = agg_utils.run_post_aggs(aggs, self.source_name)

        return aggs

    def chunked_scan(self):
        """Fetch and aggregate HDFS records in month-long chunks"""
        # Save min/max datetime states locally
        min_month = self.min_datetime.month
        max_month = self.max_datetime.month
        if min_datetime.year != max_datetime.year:
            min_year = self.min_datetime.year
            max_year = self.max_datetime.year
            # Min to end of min year
            aggs = scan_month_range(min_month, 12, min_year)
            # Start of max year to max
            aggs += scan_month_range(1, max_month, max_year)
            aggs = agg_utils.run_post_aggs(aggs, self.source_name)
        else:
            year = self.min_datetime.year
            result = scan_month_range(min_month, max_month, year) 
        return result

    def scan_month_range(min_month, max_month, year):
        """Fetch and aggregate monthly HDFS records from a range of 
           months within the same year
        """
        for m in range(min_month, max_month+1):
            # Get month info
            month = "0"+str(m) if m < 10 else str(m)
            end = monthrange(year, m)[1]

            # Make datetime objects
            min_str = "{0} 01 00:00:00 {1} UTC".format(month, year)
            min_datetime = parser.parse(min_str)
            max_str = "{0} {1} 23:59:59 {2} UTC".format(month, end, year)
            max_datetime = parser.parse(max_str)

            # Fetch HDFS records
            self.min_datetime = min_datetime
            self.max_datetime = max_datetime
            ds = self.fetch()
            if not ds:
                continue
            else:
                df = ds.toPandas()
                aggs = agg_utils.run_aggs(df, self.source_name)
                
        return aggs

    def fetch(self, save=False, out_name="hdfetchs"):
        """Fetch HDFS records between two given dates as pyspark 
           DataFrame
        """
        # Get dataset
        ds = None
        for hdfs_path in sorted(self.all_hdfs_paths()):
            ds_chunk = self.fetcher(hdfs_path)
            if not ds:
                ds = ds_chunk
            else:
                ds = ds.union(ds_chunk)

        if save:
            self.write(ds, out_name=out_name)

        return ds

    def write(self, ds, out_name="hdfetchs"):
        """Write fetched pyspark dataframe to parquet files"""
        # Generate file name
        f_date = self.get_file_date(min_datetime, max_datetime)
        f_name = ("_").join([out_name, f_date])

        # Write to parquet file on hdfs
        (ds.write.option("compression","gzip").mode("overwrite")
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

    def all_hdfs_paths(self):
        """Create full list of hadoop paths for a selection of files"""
        if self.min_datetime.year != self.max_datetime.year:
            raise ValueError("records must be from the same year")

        hdfs_paths = []
        if self.min_datetime.month == self.max_datetime.month:
            hdfs_paths = get_hdfs_paths(self.min_datetime.day,
                                        self.max_datetime.day,
                                        self.min_datetime.month,
                                        self.min_datetime.year)
        else:
            for month in range(self.min_datetime.month, self.max_datetime.month+1):
                month_start = (1 if month != self.min_datetime.month 
                               else self.min_datetime.day)
                month_end = (monthrange(self.min_datetime.year, month)[1]
                             if month != self.max_datetime.month
                             else self.max_datetime.day)
                month_paths = get_hdfs_paths(month_start, 
                                             month_end, 
                                             month,
                                             self.min_datetime.year)
                hdfs_paths += month_paths

        return hdfs_paths

    def get_hdfs_paths(self, day_min, day_max, month, year):
        """Create list of hadoop paths for a range of days in a month"""
        if day_min > day_max:
            raise ValueError("Given minimum day > maximum day")
        
        hdfs_paths = []
        day_ranges = self.get_day_ranges(day_min, day_max)
        month = str(month) if month >= 10 else "0"+str(month)
        for day_range in day_ranges:
            path = "{0}/{1}/{2}/{3}/*.{4}".format(self.hdfs_base, year, 
                                                  month, day_range, 
                                                  self.hdfs_ext)
            hdfs_paths.append(path)

        return hdfs_paths

    def get_day_ranges(self, day1, day2):
        """Create list of regex expression to cover a range of days"""
        if day1 > day2:
            raise ValueError("Given first day > last day")

        days = range(day1, day2+1)
        regexes = {}
        for d in days:
            digits = [int(i) for i in str(d)]
            is_single = (len(digits) == 1)
            tens = "0" if is_single else str(digits[0])
            ones = str(digits[0] if is_single else digits[1])
            if tens in regexes:
                cur = regexes[tens]
                if ones not in cur:
                    regexes[tens] = cur[:-1]+ones+"]"
            else:
                regexes[tens] = tens+"["+ones+"]"

        return regexes.values()

    def get_file_date(self, min_datetime, max_datetime):
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
    argparser = ArgumentParser(description="Fetch HDFS records.")
    argparser.add_argument("--datemin", type=str, default=None,
                           help="Minimum date")
    argparser.add_argument("--datemax", type=str, default=None, 
                           help="Maximum date")
    argparser.add_argument("--source", type=str, default=None, 
                           help="Name of data source")
    args = argparser.parse_args()

#     now = dt.datetime.now()
#     hdfs_base = "/project/monitoring/archive/xrootd/raw/gled"
#     hdfs_ext = "json.gz"

#     if args.datemin and args.datemax:
#         max_datetime = parser.parse(args.datemax)
#         min_datetime = parser.parse(args.datemin)

#         hdfs = hdfetchs(min_datetime, max_datetime, hdfs_base, hdfs_ext,
#                         dummy_fetch, verbose=True)
#         ds = hdfetchs.fetch()
#     else:
#         yesterday = now - dt.timedelta(days=1)
#         hdfs = hdfetchs(yesterday, now, hdfs_base, hdfs_ext,
#                         dummy_fetch, verbose=True)
#         ds = hdfs.fetch()
