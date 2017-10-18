from __future__ import absolute_import

import argparse
import csv
import sys
import logging
from signal import signal, SIGPIPE, SIG_DFL

from six import iterkeys
from future.utils import raise_from
import numpy as np
import pandas as pd

from datacapsule_crossref.collection_utils import peek

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

def calculate_counts_from_df_batches(df_batches):
  def update_stats(stats, column):
    try:
      numeric_values = to_numeric_or_input(column).dropna()
      if len(numeric_values) > 0 and np.issubdtype(numeric_values.dtype, np.number):
        stats['min'] = min(filter_none([
          stats.get('min', None),
          numeric_values.min()
        ]))
        stats['max'] = max(filter_none([
          stats.get('max', None),
          numeric_values.max()
        ]))
        stats['sum'] = stats.get('sum', 0) + numeric_values.sum()
        column_count = numeric_values.count()
        stats['count'] = stats.get('count', 0) + column_count
        column_count_non_zero = (numeric_values != 0).sum()
        stats['count_non_zero'] = stats.get('count_non_zero', 0) + column_count_non_zero
        stats['count_zero'] = stats.get('count_zero', 0) + column_count - column_count_non_zero
      else:
        stats['count'] = stats.get('count', 0) + len(column.dropna())
      return stats
    except Exception as e:
      raise_from(RuntimeError('failed to update stats for {}'.format(column)), e)
  stats_by_column = None
  num_columns = None
  for df in df_batches:
    if stats_by_column is None:
      num_columns = len(df.columns)
      stats_by_column = [dict()] * num_columns
    stats_by_column = [
      update_stats(stats_of_column.copy(), df[c])
      for stats_of_column, c in zip(stats_by_column, df.columns)
    ]
  if stats_by_column:
    stats = {}
    for i, stats_of_column in enumerate(stats_by_column):
      if 'sum' in stats_of_column and stats_of_column['count'] > 0:
        stats_of_column['mean'] = (
          stats_of_column['sum'] / stats_of_column['count']
        )
        stats_of_column['mean_non_zero'] = (
          stats_of_column['sum'] / stats_of_column['count_non_zero']
        )
      for k in iterkeys(stats_of_column):
        if not k in stats:
          stats[k] = [None] * num_columns
        this_stats = stats_of_column[k]
        stats[k][i] = stats_of_column[k]
  else:
    stats = None
  return stats

def calculate_and_output_counts(argv):
  args = get_args_parser().parse_args(argv)

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
    df_first = None
    csv_writer.writerow([''] + column_names)

  stats = calculate_counts_from_df_batches(df_batches)

  if stats:
    for k in sorted(iterkeys(stats)):
      csv_writer.writerow([k] + stats[k])

def main(argv=None):
  calculate_and_output_counts(argv)

if __name__ == "__main__":
  signal(SIGPIPE, SIG_DFL)
  logging.basicConfig(level='INFO')

  main()
