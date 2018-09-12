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

from datacapsule_crossref.utils.collection import peek

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

def calculate_counts_from_df_batches(df_batches, groupby_columns=None):
  def update_stats(stats, column):
    try:
      stats['count'] = stats.get('count', 0) + len(column)
      stats['count_valid'] = stats.get('count_valid', 0) + len(column.dropna())
      column_type = stats.get('type')
      if column_type != 'str':
        numeric_values = to_numeric_or_input(column).dropna()
        if len(numeric_values) > 0 and np.issubdtype(numeric_values.dtype, np.number):
          stats['type'] = 'numeric'
          stats['min'] = min(filter_none([
            stats.get('min', None),
            numeric_values.min()
          ]))
          stats['max'] = max(filter_none([
            stats.get('max', None),
            numeric_values.max()
          ]))
          stats['sum'] = stats.get('sum', 0) + numeric_values.sum()
          column_count_numeric = numeric_values.count()
          stats['count_numeric'] = stats.get('count_numeric', 0) + column_count_numeric
          column_count_non_zero = (numeric_values != 0).sum()
          stats['count_non_zero'] = stats.get('count_non_zero', 0) + column_count_non_zero
          stats['count_zero'] = (
            stats.get('count_zero', 0) + column_count_numeric - column_count_non_zero
          )
        elif len(column):
          if column_type == 'numeric':
            for x in {'min', 'max', 'sum', 'count_numeric', 'count_non_zero', 'count_zero'}:
              del stats[x]
          stats['type'] = 'str'
      return stats
    except Exception as e:
      raise_from(RuntimeError('failed to update stats for {}'.format(column)), e)
  stats_by_column_by_group = dict()
  num_columns = None
  for df_batch in df_batches:
    if groupby_columns:
      for g in groupby_columns:
        df_batch[g] = df_batch[g].fillna('')
      df_groups = split_and_drop_groupby_column(df_batch, groupby_columns)
    else:
      df_groups = [(None, df_batch)]
    for g, df in df_groups:
      stats_by_column = stats_by_column_by_group.get(g)
      if stats_by_column is None:
        num_columns = len(df.columns)
        stats_by_column = [dict()] * num_columns
      stats_by_column_by_group[g] = [
        update_stats(stats_of_column.copy(), df[c])
        for stats_of_column, c in zip(stats_by_column, df.columns)
      ]
  if stats_by_column_by_group:
    stats_by_group = dict()
    for g, stats_by_column in iteritems(stats_by_column_by_group):
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
            stats[k] = [None] * num_columns
          stats[k][i] = stats_of_column[k]
    if not groupby_columns:
      return stats_by_group[None]
    return stats_by_group
  else:
    stats = None
  return stats

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
    quotechar='"',
    chunksize=args.batch_size,
    header='infer' if args.header else None
  )
  if args.header:
    df_first, df_batches = peek(df_batches)
    column_names = list(df_first.columns.values)
    del df_first
    result_column_names = (
      groupby_columns + ['stat'] + [s for s in column_names if s not in groupby_columns]
    )
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
