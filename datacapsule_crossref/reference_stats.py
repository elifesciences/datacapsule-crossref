from __future__ import absolute_import

import argparse
import csv
import json
import sys
import logging
from collections import defaultdict
from signal import signal, SIGPIPE, SIG_DFL

import pandas as pd
from future.utils import raise_from

from .utils.pandas import read_csv_with_default_dtype


LOGGER = logging.getLogger(__name__)

class ProcessDataFrameError(RuntimeError):
  def __init__(self, df, message, cause):
    super(ProcessDataFrameError, self).__init__(message)
    self.df = df
    self.cause = cause

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
    '--save-failing-df', type=str,
    help='filename to save the failing dataframe to'
  )
  return parser

class CounterWithExamples(object):
  def __init__(self, limit=10):
    self.count_map = defaultdict(lambda: 0)
    self.example_map = defaultdict(list)
    self.limit = limit

  def add(self, key, example):
    previous_count = self.count_map[key]
    self.count_map[key] = previous_count + 1
    if previous_count < self.limit:
      self.example_map[key].append(example)

  def __iter__(self):
    return iter(sorted((
        (key, count, self.example_map[key])
        for key, count in self.count_map.items()
      ),
      key=lambda x: x[1],
      reverse=True
    ))

class TypedCounterWithExample(object):
  def __init__(self, limit):
    self.limit = limit
    self.counters_map = defaultdict(lambda: CounterWithExamples(self.limit))

  def add(self, counter_type, key, example):
    self.counters_map[counter_type].add(key, example)

  def __iter__(self):
    for counter_type in sorted(self.counters_map.keys()):
      for key, count, examples in self.counters_map[counter_type]:
        yield counter_type, key, count, examples


def update_typed_counter_with_batch(typed_counter_with_examples, df):
  for key, values in [
    ('publisher', df['publisher']),
    ('countainer_title', df['container_title']),
    ('first_subject_area', df['first_subject_area']),
    ('created', pd.to_datetime(df['created']).dt.year)]:
    for doi, value in zip(df['doi'], values):
      typed_counter_with_examples.add(
        'total_{}'.format(key),
        value,
        doi
      )
  df_non_oa_reference = df[(df['reference_count'] > 0) & (df['has_references'] == 0)]
  if len(df_non_oa_reference) > 0:
    for key, values in [
      ('publisher', df_non_oa_reference['publisher']),
      ('countainer_title', df_non_oa_reference['container_title']),
      ('first_subject_area', df_non_oa_reference['first_subject_area']),
      ('created', pd.to_datetime(df_non_oa_reference['created']).dt.year)]:
      for doi, value in zip(df_non_oa_reference['doi'], values):
        typed_counter_with_examples.add(
          'non_oa_ref_{}'.format(key),
          value,
          doi
        )
  for doi, debug_json in zip(df['doi'], df['debug']):
    if debug_json and not pd.isnull(debug_json):
      for reference in json.loads(debug_json):
        typed_counter_with_examples.add(
          'key_combination',
          '|'.join(sorted([
            k for k, v in reference.items()
            if v is not None and v != ""
          ])),
          (doi, reference)
        )
        for key in ['year']:
          typed_counter_with_examples.add(
            key,
            reference.get(key),
            (doi, reference)
          )

def calculate_counts_from_rows(df_batches):
  typed_counter_with_examples = TypedCounterWithExample(10)
  for df in df_batches:
    LOGGER.info('processing batch: %s', df.shape)
    try:
      update_typed_counter_with_batch(typed_counter_with_examples, df)
    except Exception as e:
      raise_from(ProcessDataFrameError(df, "failed to process batch data frame", e), e)
  return typed_counter_with_examples

def calculate_and_output_counts(argv):
  args = get_args_parser().parse_args(argv)

  csv.field_size_limit(min(2147483647, sys.maxsize))

  csv_writer = csv.writer(sys.stdout, delimiter=args.delimiter)

  # only set the column types we need, otherwise use object
  # (this will be faster and more reliable than inferring the column type)
  column_dtype = {
    'reference_count': int,
    'has_references': int
  }
  LOGGER.info('column_dtype: %s', column_dtype)
  LOGGER.info('batch size: %s', args.batch_size)

  df_batches = read_csv_with_default_dtype(
    sys.stdin,
    sep=args.delimiter,
    chunksize=args.batch_size,
    dtype=column_dtype,
    default_dtype='object'
  )

  try:
    typed_counter_with_examples = calculate_counts_from_rows(df_batches)
  except ProcessDataFrameError as e:
    if args.save_failing_df:
      e.df.to_csv(args.save_failing_df, sep=args.delimiter)
      LOGGER.info('saved failing dataframe to: %s', args.save_failing_df)
    raise e

  csv_writer.writerow(['type', 'key', 'count', 'examples'])
  for counter_type, key, count, examples in typed_counter_with_examples:
    csv_writer.writerow([counter_type, key, count, json.dumps(examples)])

def setup():
  logging.basicConfig(level='INFO')
  signal(SIGPIPE, SIG_DFL)

def main(argv=None):
  calculate_and_output_counts(argv)

if __name__ == "__main__":
  setup()

  main()
