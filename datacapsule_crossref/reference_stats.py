from __future__ import absolute_import

import argparse
import csv
import json
import sys
from signal import signal, SIGPIPE, SIG_DFL

from six import iteritems
import pandas as pd

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

class CounterWithExamples():
  def __init__(self, limit=10):
    self.count_map = dict()
    self.example_map = dict()
    self.limit = limit
  
  def add(self, key, example):
    previous_count = self.count_map.get(key, 0)
    self.count_map[key] = previous_count + 1
    if previous_count < self.limit:
      if not key in self.example_map:
        self.example_map[key] = []
      self.example_map[key].append(example)

  def __iter__(self):
    return (
      (key, count, self.example_map.get(key))
      for key, count in iteritems(self.count_map)
    )

def calculate_counts_from_rows(df_batches):
  counter_with_examples = CounterWithExamples(10)
  for df in df_batches:
    for doi, debug_json in zip(df['doi'], df['debug']):
      if debug_json and not pd.isnull(debug_json):
        for reference in json.loads(debug_json):
          counter_with_examples.add(
            '|'.join(sorted([
              k for k, v in iteritems(reference)
              if v is not None and v != ""
            ])),
            (doi, reference)
          )
  return counter_with_examples

def calculate_and_output_counts(argv):
  args = get_args_parser().parse_args(argv)

  csv_writer = csv.writer(sys.stdout, delimiter=args.delimiter)

  df_batches = pd.read_csv(
    sys.stdin,
    sep=args.delimiter,
    chunksize=args.batch_size
  )

  counter_with_examples = calculate_counts_from_rows(df_batches)
  csv_writer.writerow(['key_combination', 'count', 'examples'])

  for key_combination, count, examples in counter_with_examples:
    csv_writer.writerow([key_combination, count, json.dumps(examples)])

def main(argv=None):
  calculate_and_output_counts(argv)

if __name__ == "__main__":
  signal(SIGPIPE, SIG_DFL)

  main()
