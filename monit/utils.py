from dateutil import parser
from calendar import monthrange

def get_number_string(num):
    """Return string representation of a number appropriate for a date"""
    return "0"+str(num) if num < 10 else str(num)

def get_all_file_paths(base, ext, min_datetime, max_datetime):
    """Create full list of hadoop-like paths for a selection of files"""
    if base[-1] == "/":
        base = base[:-1]
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
        path = "{0}/{1}/{2}/{3}/*.{4}".format(base, year, month, 
                                              day_range, ext)
        paths.append(path)

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
