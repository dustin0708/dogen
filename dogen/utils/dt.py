#-*-coding:utf-8-*-

import sys
import time
import datetime


def date_today():
    """Get date like 'yy-mm-dd'
    """
    return datetime.date.today().isoformat()

def time_now():
    """Get time like 'hh:mm:ss'.
    """
    return time.strftime("%H:%M:%S")
    
def datetime_now():
    """Get datetime like 'yy-mm-dd hh:mm:ss'
    """
    return date_today() + ' ' + time_now()

def date_delta(date, num):
    """Caculate new date by add the offset(num) base current date.
        
    Args:
        date: the base date with format YYYY-MM-DD.
        num : integer data.
    
    Returns:
        date with format YYYY-MM-DD
    """
    tc = time.strptime(date, "%Y-%m-%d")
    to = datetime.date(tc.tm_year, tc.tm_mon, tc.tm_mday) + datetime.timedelta(num)
    return to.strftime("%Y-%m-%d")


def date_from_int(date):
    """Convert date like 'YYYY-MM-DD' from integer date like YYYYMMDD
    """
    return time.strftime("%Y-%m-%d", time.strptime(str(date), "%Y%m%d"))


def date_into_int(date):
    """Convert date like "YYYY-MM-DD" to integer date like YYYYMMDD
    """
    try:
        val = int(time.strftime("%Y%m%d", time.strptime(date, "%Y-%m-%d")))
    except Exception:
        val = 0
    return val

def time_diff(t1, t2):
    """Get difference seconds by 't1-t2'.
    
    Args:
        t1: time like 'hh:mm:ss'.
        t2: time like 'hh:mm:ss'.
    
    Returns:
        Seconds integer value.
    """
    tmp1 = time.strptime(t1,"%H:%M:%S")
    tmp2 = time.strptime(t2,"%H:%M:%S")
    return int(time.mktime(tmp1) - time.mktime(tmp2))
