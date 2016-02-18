import calendar
import dateutil.parser
import datetime
import time
import numpy as np
try:
    import netCDF4
except ImportError:
    pass


class IonDate(datetime.date):
    """
    Factory class to generate (tz unaware) datetime objects from various input formats
    """
    def __new__(cls, *args):
        if len(args) == 3:
            return datetime.date.__new__(cls, *args)
        elif len(args) == 1:
            if isinstance(args[0], basestring):
                dt = datetime.datetime.strptime(args[0], '%Y-%m-%d')
                return datetime.date.__new__(cls, dt.year, dt.month, dt.day)
            elif isinstance(args[0], datetime.date):
                dt = args[0]
                return datetime.date.__new__(cls, dt.year, dt.month, dt.day)
        raise TypeError('Required arguments are (int,int,int) or (str) in the "YYYY-MM-DD" pattern')
