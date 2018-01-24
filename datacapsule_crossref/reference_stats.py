from __future__ import absolute_import

import argparse
import csv
import json
import sys
from signal import signal, SIGPIPE, SIG_DFL

from six import iteritems, iterkeys
import pandas as pd

class Columns(object):
  TYPE = 'type'
  KEY = 'key'
  COUNT = 'count'
  EXAMPLES = 'examples'

REFERENCE_STATS_COLUMNS = [Columns.TYPE, Columns.KEY, Columns.COUNT, Columns.EXAMPLES]

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

class CounterWithExamples(object):
  def __init__(self, limit=10):
    self.count_map = dict()
    self.example_map = dict()
    self.limit = limit

  def add_counter(self, counter):
    for key, count in iteritems(counter.count_map):
      self.count_map[key] = self.count_map.get(key, 0) + count
      examples = self.example_map.get(key, [])
      slots_left = self.limit - len(examples)
      for example in counter.example_map.get(key, [])[:slots_left]:
        examples.append(example)
      self.example_map[key] = examples


  def add(self, key, example):
    previous_count = self.count_map.get(key, 0)
    self.count_map[key] = previous_count + 1
    if previous_count < self.limit:
      if not key in self.example_map:
        self.example_map[key] = []
      self.example_map[key].append(example)

  def __iter__(self):
    return iter(sorted((
        (key, count, self.example_map.get(key))
        for key, count in iteritems(self.count_map)
      ),
      key=lambda x: x[1],
      reverse=True
    ))

class TypedCounterWithExamples(object):
  def __init__(self, limit):
    self.counters_map = {}
    self.limit = limit

  def add_counter(self, typed_counter):
    for counter_type, other_counter in iteritems(typed_counter.counters_map):
      counter = self.counters_map.get(counter_type)
      if counter is None:
        counter = CounterWithExamples(self.limit)
        self.counters_map[counter_type] = counter
      counter.add_counter(other_counter)

  def add(self, counter_type, key, example):
    counter_with_examples = self.counters_map.get(counter_type)
    if counter_with_examples is None:
      counter_with_examples = CounterWithExamples(self.limit)
      self.counters_map[counter_type] = counter_with_examples
    counter_with_examples.add(key, example)

  def __iter__(self):
    for counter_type in sorted(iterkeys(self.counters_map)):
      for key, count, examples in self.counters_map[counter_type]:
        yield counter_type, key, count, examples

def update_counter_with_references(typed_counter_with_examples, doi, references):
  for reference in references:
    typed_counter_with_examples.add(
      'key_combination',
      '|'.join(sorted([
        k for k, v in iteritems(reference)
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
  typed_counter_with_examples = TypedCounterWithExamples(10)
  for df in df_batches:
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
        update_counter_with_references(
          typed_counter_with_examples,
          doi,
          json.loads(debug_json)
        )
  return typed_counter_with_examples

def calculate_and_output_counts(argv):
  args = get_args_parser().parse_args(argv)

  csv_writer = csv.writer(sys.stdout, delimiter=args.delimiter)

  df_batches = pd.read_csv(
    sys.stdin,
    sep=args.delimiter,
    chunksize=args.batch_size
  )

  typed_counter_with_examples = calculate_counts_from_rows(df_batches)
  csv_writer.writerow(REFERENCE_STATS_COLUMNS)

  for counter_type, key, count, examples in typed_counter_with_examples:
    csv_writer.writerow([counter_type, key, count, json.dumps(examples)])

def main(argv=None):
  calculate_and_output_counts(argv)

if __name__ == "__main__":
  signal(SIGPIPE, SIG_DFL)

  main()
