from __future__ import absolute_import

import argparse
import csv
import sys
import re
from signal import signal, SIGPIPE, SIG_DFL

from six import iterkeys, iteritems

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Automatically calculate sums of numeric values'
  )
  parser.add_argument(
    '--delimiter', type=str, default='\t',
    help='delimiter to use'
  )
  parser.add_argument(
    '--header', required=False,
    action='store_true',
    help='whether the input contains a header row'
  )
  return parser

def calculate_counts_from_rows(rows):
  num_pattern = re.compile(r'^\d+$')
  stat_functions = {
    'min': min,
    'max': max,
    'sum': lambda agg, value: agg + value,
    'count': lambda agg, value: agg + 1,
    'count_non_zero': lambda agg, value: agg + 1 if value else agg,
    'count_zero': lambda agg, value: agg + 1 if not value else agg
  }
  stats = None
  for row in rows:
    if stats is None:
      stats = {
        k: [None] * len(row)
        for k in iterkeys(stat_functions)
      }
    for i, value in enumerate(row):
      if value == 'true':
        value = '1'
      if value == 'false':
        value = '0'
      if num_pattern.match(value):
        for k, stat_function in iteritems(stat_functions):
          stats[k][i] = stat_function(
            stats[k][i] or 0,
            int(value)
          )
  if stats:
    stats['mean'] = [
      sum_value / count_value if count_value else None
      for sum_value, count_value in zip(stats['sum'], stats['count'])
    ]
    stats['mean_non_zero'] = [
      sum_value / count_value if count_value else None
      for sum_value, count_value in zip(stats['sum'], stats['count_non_zero'])
    ]
  return stats

def calculate_and_output_counts(argv):
  args = get_args_parser().parse_args(argv)

  csv.field_size_limit(sys.maxsize)
  csv_reader = csv.reader(sys.stdin, delimiter=args.delimiter)
  csv_writer = csv.writer(sys.stdout, delimiter=args.delimiter)

  if args.header:
    csv_writer.writerow([''] + next(csv_reader))

  stats = calculate_counts_from_rows(csv_reader)

  if stats:
    for k in sorted(iterkeys(stats)):
      csv_writer.writerow([k] + stats[k])

def main(argv=None):
  calculate_and_output_counts(argv)

if __name__ == "__main__":
  signal(SIGPIPE, SIG_DFL)

  main()
