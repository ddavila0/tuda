import argparse
import datetime as dt
from os import environ
from dateutil import parser as dparser
from calendar import monthrange

class HDFetchS:
    """HDFS Dynamic Fetch Script (HDFetchS)"""

    def __init__(self, date_min, date_max, hdfs_base, hdfs_ext,
                 fetcher, out_name="hdfetchs", verbose=False):
        self.date_min = date_min
        self.date_max = date_max
        self.hdfs_base = hdfs_base
        self.hdfs_ext = hdfs_ext
        self.fetcher = fetcher
        self.v = verbose
        self.ds = None

        self.fname = ("_").join([out_name, self.file_date(date_min, date_max)])
        self.hdfs_paths = sorted(self.hadoop_paths())

        if hdfs_base[-1] == "/":
            self.hdfs_base = hdfs_base[:-1]
        if hdfs_ext[0] == ".":
            self.hdfs_ext = hdfs_ext[1:]
        return

    def write(self):
        """Fetch XRootD records between two given dates"""
        # Fetch pyspark dataframe
        if not self.ds:
            print("WARNING: self.ds is empty, no dataframe written")
            return

        # Write to parquet file on hdfs
        (self.ds.write
                .option("compression","gzip")
                .mode("overwrite")
                .parquet(self.fname)
        )

        # Move from hdfs to eos
        user = environ["USER"]
        hdfs_loc = "/user/{0}/shared/{1}".format(user, self.fname)
        eos_dest = "/eos/user/{0}/{1}/{2}".format(user[0], user, 
                                                  self.fname)
        p = Popen("hdfs dfs -get {0} {1}".format(hdfs_loc, eos_dest),
                  shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, 
                  close_fds=True)

        if self.v:
            print(p.stdout.read())
            print("Wrote to {}".format(eos_dest))
        return

    def fetch(self):
        """Fetch HDFS records between two given dates as pyspark DataFrame"""
        # Get dataset
        ds = None
        for hdfs_path in self.hdfs_paths:
            _ds = self.fetcher(hdfs_path)
            if not ds:
                ds = _ds
            else:
                ds = ds.union(_ds)
            del _ds

        self.ds = ds
        return

    def hadoop_paths(self):
        """Create full list of hadoop paths to fetch a selection of files"""
        if self.date_min.year != self.date_max.year:
            raise ValueError("records must be from the same year")

        hdfs_paths = []
        if self.date_min.month == self.date_max.month:
            hdfs_paths = self.paths(self.date_min.day, self.date_max.day,
                                    self.date_min.month, self.date_min.year,
                                    self.hdfs_base, hdfs_ext)
        else:
            for month in range(self.date_min.month, self.date_max.month+1):
                month_start = (1 if month != self.date_min.month 
                               else self.date_min.day)
                month_end = (monthrange(self.date_min.year, month)[1]
                             if month != self.date_max.month
                             else self.date_max.day)
                month_paths = self.paths(month_start, month_end, month,
                                         self.date_min.year, 
                                         self.hdfs_base, self.hdfs_ext)
                hdfs_paths += month_paths

        return hdfs_paths

    def paths(self, day_min, day_max, month, year, hdfs_base, hdfs_ext):
        """Create list of hadoop paths for a range of days in a month"""
        if day_min > day_max:
            raise ValueError("Given minimum day > maximum day")
        
        hdfs_paths = []
        dayrange = self._day_range(day_min, day_max)
        month = str(month) if month >= 10 else "0"+str(month)
        for dr in dayrange:
            hdfs_paths.append("{0}/{1}/{2}/{3}/*.{4}".format(hdfs_base, 
                                                             year,
                                                             month, dr,
                                                             hdfs_ext))
        return hdfs_paths

    def _day_range(self, day1, day2):
        """Create list of regex expression to cover a range of days"""
        if day1 > day2:
            raise ValueError("Given first day > last day")

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

    def file_date(self, date_min, date_max):
        """Create a unique string for naming output files"""
        if date_min > date_max:
            raise ValueError("given minimum date > maximum date")

        datestr_min = date_min.strftime("%m-%d-%Y")
        datestr_max = date_max.strftime("%m-%d-%Y")
        file_date = ""

        if date_min and date_max:
            same_m = date_min.strftime("%m") == date_max.strftime("%m")
            same_d = date_min.strftime("%d") == date_max.strftime("%d")
            same_y = date_min.strftime("%Y") == date_max.strftime("%Y")
            if same_m and same_d and same_y:
                file_date = datestr_min
            elif same_m and same_y:
                file_date = "{0}-{1}to{2}-{3}".format(date_min.strftime("%m"),
                                                      date_min.strftime("%d"),
                                                      date_max.strftime("%d"),
                                                      date_min.strftime("%Y"))
            elif same_y:
                file_date = "{0}-{1}to{2}".format(date_min.strftime("%m"),
                                                  date_min.strftime("%d"),
                                                  datestr_max)
            else:
                file_date = "{0}to{1}".format(datestr_min, datestr_max)
                
        return file_date


def dummy_fetch(path):
    print("Fetched",path)
    return None

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

        hdfetchs = HDFetchS(date_min, date_max, hdfs_base, hdfs_ext,
                            dummy_fetch, verbose=True)
        hdfetchs.fetch()
        print(hdfetchs.fname)
    else:
        yesterday = now - dt.timedelta(days=1)
        hdfetchs = HDFetchS(yesterday, now, hdfs_base, hdfs_ext,
                            dummy_fetch, verbose=True)
        hdfetchs.fetch()
        print(hdfetchs.fname)
