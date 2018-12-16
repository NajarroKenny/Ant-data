
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
import pandas as pd

from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def get_local_date(country, as_str=True):
   if country not in COUNTRY_LIST:
       raise Exception(f'{country} is not a valid country')

   tz = TZ.get(country)
   local_date = pd.Timestamp.now(tz=tz).date()
   return local_date.isoformat() if as_str else local_date


def shift_date(date, amount=1, unit='days'):
   date_object = datetime.date.fromisoformat(date)
   
   switcher = {
       'hours': datetime.timedelta(hours=amount),
       'days': datetime.timedelta(days=amount),
       'weeks': datetime.timedelta(weeks=amount)
   }

   return date_object + switcher.get(unit)

def shift_date_object(date_object, amount=1, unit='days'):
   switcher = {
       'hours': datetime.timedelta(hours=amount),
       'days': datetime.timedelta(days=amount),
       'weeks': datetime.timedelta(weeks=amount)
   }

   return date_object + switcher.get(unit)
