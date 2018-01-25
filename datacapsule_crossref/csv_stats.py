from __future__ import absolute_import
from __future__ import division

import argparse
import csv
import sys
import logging
from signal import signal, SIGPIPE, SIG_DFL

from six import iterkeys, iteritems
from future.utils import raise_from
import numpy as np
import pandas as pd

from datacapsule_crossref.utils.collection import peek, extend_dict

def get_logger():
  return logging.getLogger(__name__)

class Columns(object):
  STAT = 'stat'

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Automatically calculate sums of numeric values'
  )
  parser.add_argument(
    '--delimiter', type=str, default='\t',
    help='delimiter to use'
  )
  parser.add_argument(
    '--batch-size', type=int, default=100 * 1000,
    help='batch size to use (rows)'
  )
  parser.add_argument(
    '--header', required=False,
    action='store_true',
    help='whether the input contains a header row'
  )
  parser.add_argument(
    '--group-by', type=str, required=False,
    help='group by column'
  )
  return parser

def filter_none(l):
  return [v for v in l if v is not None]

def bool_literal_to_number(df):
  if not np.issubdtype(df.dtype, np.number):
    return df.replace({'True': 1, 'true': 1, 'False': 0, 'false': 0})
  else:
    return df

def to_numeric_or_input(df):
  return pd.to_numeric(bool_literal_to_number(df), errors='ignore').dropna()

def split_and_drop_groupby_column(df, groupby_column):
  gb = df.groupby(groupby_column)
  return [
    (x, gb.get_group(x).drop(groupby_column, axis=1))
    for x in gb.groups
  ]

def safe_mean(sum_value, count):
  return sum_value / count if count else 0

def _get_column_stats_for_column(column, previous_type=None):
  stats = dict()
  try:
    stats['count'] = len(column)
    stats['count_valid'] = len(column.dropna())
    if previous_type != 'str':
      numeric_values = to_numeric_or_input(column).dropna()
      if len(numeric_values) > 0 and np.issubdtype(numeric_values.dtype, np.number):
        stats['type'] = 'numeric'
        stats['min'] = numeric_values.min()
        stats['max'] = numeric_values.max()
        stats['sum'] = numeric_values.sum()
        column_count_numeric = numeric_values.count()
        stats['count_numeric'] = column_count_numeric
        column_count_non_zero = (numeric_values != 0).sum()
        stats['count_non_zero'] = column_count_non_zero
        stats['count_zero'] = column_count_numeric - column_count_non_zero
      elif len(column):
        stats['type'] = 'str'
    else:
      stats['type'] = previous_type
    return stats
  except Exception as e:
    raise_from(RuntimeError('failed to update stats for {}'.format(column)), e)

def _get_column_stats_for_values(values, previous_type=None):
  return _get_column_stats_for_column(pd.Series(values), previous_type)

def _merge_column_stats(column_stats, other_column_stats):
  if not other_column_stats:
    return column_stats

  if not column_stats:
    return other_column_stats

  get_logger().debug('column_stats: %s', column_stats)

  column_type = column_stats.get('type')
  merged_column_stats = dict()
  value_columns = column_stats.keys()
  if column_type == 'numeric' and other_column_stats.get('type') != 'numeric':
    merged_column_stats['type'] = 'str'
    value_columns = (
      set(value_columns) -
      {'min', 'max', 'sum', 'count_numeric', 'count_non_zero', 'count_zero'}
    )
  else:
    merged_column_stats['type'] = column_type
  for k in value_columns:
    if k.startswith('count') or k == 'sum':
      merged_column_stats[k] = column_stats.get(k, 0) + other_column_stats.get(k, 0)
    elif k == 'min':
      merged_column_stats[k] = min(column_stats.get(k, 0), other_column_stats.get(k, 0))
    elif k == 'max':
      merged_column_stats[k] = max(column_stats.get(k, 0), other_column_stats.get(k, 0))
  return merged_column_stats

def _merge_column_list_stats(column_list_stats, other_column_list_stats):
  if not other_column_list_stats:
    return column_list_stats
  if not column_list_stats:
    return other_column_list_stats
  return [
    _merge_column_stats(column_stats, other_column_stats)
    for column_stats, other_column_stats in zip(column_list_stats, other_column_list_stats)
  ]

def _get_and_merge_column_stats(column_stats, column):
  other_column_stats = _get_column_stats_for_column(column)
  merged_column_stats = _merge_column_stats(column_stats, other_column_stats)
  return merged_column_stats

class CsvStats(object):
  def __init__(self, groupby_columns=None):
    self.groupby_columns = groupby_columns
    self.stats_by_column_by_group = dict()
    self.num_columns = None

  def add_dataframe(self, df_batch):
    if self.groupby_columns:
      for g in self.groupby_columns:
        df_batch[g] = df_batch[g].fillna('')
      df_groups = split_and_drop_groupby_column(df_batch, self.groupby_columns)
    else:
      df_groups = [(None, df_batch)]
    for g, df in df_groups:
      stats_by_column = self.stats_by_column_by_group.get(g)
      if stats_by_column is None:
        self.num_columns = len(df.columns)
        stats_by_column = [dict()] * self.num_columns
      self.stats_by_column_by_group[g] = [
        _get_and_merge_column_stats(stats_of_column, df[c])
        for stats_of_column, c in zip(stats_by_column, df.columns)
      ]

  def add_dict_list(self, dict_list, column_names):
    for g, grouped_dict_list in zip([None], [dict_list]):
      stats_by_column = self.stats_by_column_by_group.get(g)
      if stats_by_column is None:
        self.num_columns = len(column_names)
        stats_by_column = [dict()] * self.num_columns
      self.stats_by_column_by_group[g] = [
        _merge_column_stats(
          stats_of_column,
          _get_column_stats_for_values(
            [d.get(c) for d in grouped_dict_list],
            stats_of_column.get('type')
          )
        )
        for stats_of_column, c in zip(stats_by_column, column_names)
      ]

  def add_stats(self, other_csv_stats):
    if not other_csv_stats.stats_by_column_by_group:
      return

    assert self.groupby_columns == other_csv_stats.groupby_columns

    if not self.num_columns:
      self.num_columns = other_csv_stats.num_columns
    assert self.num_columns == other_csv_stats.num_columns

    group_keys = (
      set(self.stats_by_column_by_group.keys()) |
      set(other_csv_stats.stats_by_column_by_group.keys())
    )
    for g in group_keys:
      self.stats_by_column_by_group[g] = _merge_column_list_stats(
        self.stats_by_column_by_group.get(g),
        other_csv_stats.stats_by_column_by_group.get(g)
      )

  def get_stats(self):
    if self.stats_by_column_by_group:
      stats_by_group = dict()
      get_logger().debug('stats_by_column_by_group: %s', self.stats_by_column_by_group)
      for g, stats_by_column in iteritems(self.stats_by_column_by_group):
        stats = stats_by_group.setdefault(g, dict())
        for i, stats_of_column in enumerate(stats_by_column):
          if 'sum' in stats_of_column and stats_of_column['count_numeric'] > 0:
            stats_of_column['mean'] = safe_mean(
              stats_of_column['sum'], stats_of_column['count_numeric']
            )
            stats_of_column['mean_non_zero'] = safe_mean(
              stats_of_column['sum'], stats_of_column['count_non_zero']
            )
          for k in iterkeys(stats_of_column):
            if not k in stats:
              stats[k] = [None] * self.num_columns
            stats[k][i] = stats_of_column[k]
      if not self.groupby_columns:
        return stats_by_group[None]
      return stats_by_group
    else:
      stats = None
    return stats

def calculate_counts_from_df_batches(df_batches, groupby_columns=None):
  csv_stats = CsvStats(groupby_columns=groupby_columns)
  for df_batch in df_batches:
    csv_stats.add_dataframe(df_batch)
  return csv_stats.get_stats()

def get_output_column_names(column_names, groupby_columns):
  if groupby_columns is None:
    groupby_columns = []
  return (
    groupby_columns +
    [Columns.STAT] +
    [s for s in column_names if s not in groupby_columns]
  )

def flatten_stats(stats, column_names, groupby_columns):
  if not stats:
    return
  if groupby_columns:
    column_names_excl_groupby_columns = [c for c in column_names if c not in groupby_columns]
    for g in sorted(iterkeys(stats)):
      g_column_values = list(g) if isinstance(g, tuple) else [g]
      stats_group = stats[g]
      for k in sorted(iterkeys(stats_group)):
        yield extend_dict(
          {c: v for c, v in zip(groupby_columns, g_column_values)},
          {Columns.STAT: k},
          {c: v for c, v in zip(column_names_excl_groupby_columns, stats_group[k])}
        )
  else:
    for k in sorted(iterkeys(stats)):
      yield extend_dict(
        {Columns.STAT: k},
        {c: v for c, v in zip(column_names, stats[k])}
      )

def calculate_and_output_counts(argv):
  args = get_args_parser().parse_args(argv)
  groupby_columns = (
    [s.strip() for s in args.group_by.split(',')]
    if args.group_by
    else []
  )

  csv_writer = csv.writer(sys.stdout, delimiter=args.delimiter)

  df_batches = pd.read_csv(
    sys.stdin,
    sep=args.delimiter,
    chunksize=args.batch_size,
    header='infer' if args.header else None
  )
  if args.header:
    df_first, df_batches = peek(df_batches)
    column_names = list(df_first.columns.values)
    del df_first
    result_column_names = get_output_column_names(column_names, groupby_columns)
    csv_writer.writerow(result_column_names)

  stats = calculate_counts_from_df_batches(df_batches, groupby_columns=groupby_columns)

  if stats:
    if groupby_columns:
      for g in sorted(iterkeys(stats)):
        g_column_values = list(g) if isinstance(g, tuple) else [g]
        stats_group = stats[g]
        for k in sorted(iterkeys(stats_group)):
          csv_writer.writerow(g_column_values + [k] + stats_group[k])
    else:
      for k in sorted(iterkeys(stats)):
        csv_writer.writerow([k] + stats[k])

def main(argv=None):
  calculate_and_output_counts(argv)

if __name__ == "__main__":
  signal(SIGPIPE, SIG_DFL)
  logging.basicConfig(level='INFO')

  main()
