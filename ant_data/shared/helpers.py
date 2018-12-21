
"""
Helpers
==========================
Commonly used generic data functions

- Create date:  2018-12-16
- Update date:  2018-12-16
- Version:      !.0

Notes:
==========================
- v1.0: Initial version
"""
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

from ant_data.static.GEOGRAPHY import COUNTRY_LIST
from ant_data.static.TIME import TZ


def local_date_str(country):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    tz = TZ.get(country)
    local_date = pd.Timestamp.now(tz=tz).date()
    return local_date.isoformat()

def local_date_dt(country):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    tz = TZ.get(country)
    local_date = pd.Timestamp.now(tz=tz).date()
    return local_date


def shift_date_str(date_str, days=0, weeks=0, months=0, years=0):
    date_dt = datetime.date.fromisoformat(date_str)
    shifted_dt = date_dt + relativedelta(days=days, weeks=weeks, months=months, years=years)
    shifted_str = shifted_dt.isoformat()

    return shifted_str


def shift_date_dt(date_dt, days=0, weeks=0, months=0, years=0):
    shifted_dt = date_dt + relativedelta(days=days, weeks=weeks, months=months, years=years)

    return shifted_dt


def date_str(date_dt):
    return date_dt.isoformat()


def date_dt(date_str):
    return datetime.date.fromisoformat(date_str)


def start_interval_str(date_str, interval):
    date = datetime.date.fromisoformat(date_str)
    if interval == 'day':
        pass
    elif interval == 'week':
        date = date + pd.DateOffset(days=(7 - date.isoweekday()))
    elif interval == 'month':
        date = date - pd.DateOffset(days=(date.day-1))
    elif interval == 'quarter':
        qdate = (date.month - 1) // 3 + 1
        date = datetime.datetime(date.year, 3 * qdate - 2, 1)
    elif interval == 'year':
        date = datetime.datetime(date.year, 1, 1)

    return date.date().isoformat()


def end_interval_str(date_str, interval):
    date = datetime.date.fromisoformat(date_str)
    if interval == 'day':
        pass
    elif interval == 'week':
        date = date + pd.DateOffset(days=(7 - date.isoweekday()))
    elif interval == 'month':
        if not pd.Timestamp(date).is_month_end:
            date = date + pd.offsets.MonthEnd()
    elif interval == 'quarter':
        if not pd.Timestamp(date).is_quarter_end:
            date = date + pd.offsets.QuarterEnd()
    elif interval == 'year':
        if not pd.Timestamp(date).is_year_end:
            date = date + pd.offsets.YearEnd()

    return date.date().isoformat()


def start_interval_dt(date, interval):
    if interval == 'day':
        pass
    elif interval == 'week':
        date = date + pd.DateOffset(days=(7 - date.isoweekday()))
    elif interval == 'month':
        date = date - pd.DateOffset(days=(date.day-1))
    elif interval == 'quarter':
        qdate = (date.month - 1) // 3 + 1
        date = datetime.datetime(date.year, 3 * qdate - 2, 1)
    elif interval == 'year':
        date = datetime.datetime(date.year, 1, 1)

    return date


def end_interval_dt(date, interval):
    if interval == 'day':
        pass
    elif interval == 'week':
        date = date + pd.DateOffset(days=(7 - date.isoweekday()))
    elif interval == 'month':
        if not pd.Timestamp(date).is_month_end:
            date = date + pd.offsets.MonthEnd()
    elif interval == 'quarter':
        if not pd.Timestamp(date).is_quarter_end:
            date = date + pd.offsets.QuarterEnd()
    elif interval == 'year':
        if not pd.Timestamp(date).is_year_end:
            date = date + pd.offsets.YearEnd()

    return date

# TODO:P2
def convert_timestamp_local(timestamp):
    pass
