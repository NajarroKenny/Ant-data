"""
Credits by Plan
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on credits by plan

- Create date:  2018-11-30
- Update date:  2018-12-31
- Version:      1.2

Notes:
==========================
- v1.0: Initial version
- v1.1: Updated with standard based on v1.1 of systems_opened
- v1.2: g/f pattern to avoid side effects
"""
from copy import deepcopy

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.codes import codes__plan


def df(
  doctype, country, agents, start=None, end=None, f=None, interval='month', paid=True, free=True, iva=True,
  commission=True
):
  g = [] if f is None else deepcopy(f)

  if not isinstance(agents, list):
    agents = [agents]
  g.append(Q('terms', agent_id=agents))

  return codes__plan.df(country, doctype, start=start, end=end, f=g, interval=interval)


