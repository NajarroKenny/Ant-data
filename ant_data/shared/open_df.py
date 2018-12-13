from pandas import concat, DataFrame, date_range, MultiIndex, offsets, Series, Timestamp
import datetime


def open_df(opened, closed, open_now, interval, method):
  if opened.empty or closed.empty:
    return DataFrame()

  # Date range interval
  intervals = {
    'year': 'YS',
    'quarter': 'QS',
    'month': 'MS',
    'week': 'W-MON',
    'day': 'D'
  }
  start_date = min(opened.index.min(), closed.index.min())
  end_date = Timestamp.now().date().isoformat()
  idx = date_range(start_date, end_date, freq=intervals[interval])

  # Merge opened and closed into one df, with separate columns
  merged_df = DataFrame(index=idx)
  merged_df.index.name = 'date'
  merged_df = merged_df.merge(opened, on='date', how='outer')
  merged_df = merged_df.merge(closed, on='date', how='outer')
  merged_df = merged_df.set_index(idx)
  merged_df = merged_df.fillna(0).astype('int64')

  # Subtract columns to get delta
  delta = DataFrame(index=idx)
  models = set(list(opened) + list(closed))
  for model in models:
    left = 0
    if f'{model}_x' in list(merged_df):
      left = merged_df[f'{model}_x']
    right = 0
    if f'{model}_y' in list(merged_df):
      right = merged_df[f'{model}_y']
    delta[model] = left - right

  # Shift if required
  if method == 'end':
    delta = delta.shift(-1)
    delta = delta.fillna(0).astype('int64')

  # Add open now to first row of delta
  for model in models:
    delta.at[delta.index.values[-1], model] += \
      open_now.loc[model].at['now'] if model in open_now.index else 0

  # Cumulative sum of delta
  n = len(delta.index)
  for i in reversed(range(0, n - 1)):
    delta.iloc[i] = delta.iloc[i+1] - delta.iloc[i]

  return delta