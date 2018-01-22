from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from crossref.restful import Works, Etiquette

from datacapsule_crossref.beam_utils.main import (
  add_cloud_args,
  process_cloud_args
)

from datacapsule_crossref.download_works_utils import (
  get_published_year_counts,
  group_year_counts_to_filters_by_target,
  save_items_from_endpoint_for_filter_to_zipfile
)

from datacapsule_crossref.download_works import (
  LZMA, BZIP2, DEFLATE
)

APPLICATION_NAME = 'DataCapsule Crossref'
APPLICATION_VERSION = '0.0.1'
APPLICATION_URL = 'https://github.com/elifesciences/datacapsule-crossref'

def get_logger():
  return logging.getLogger(__name__)

def get_target_filter_map(works_endpoint, opt):
  if opt.group_by_published_date:
    year_counts = get_published_year_counts(works_endpoint)
    return group_year_counts_to_filters_by_target(
      year_counts
    )
  return {
    opt.filter_name: opt.filter
  }

def get_works_endpoint(opt):
  if opt.email:
    return Works(etiquette=Etiquette(
      APPLICATION_NAME, APPLICATION_VERSION, APPLICATION_URL, opt.email
    ))
  return Works()

class RetrieveAndSaveWorks(object):
  def __init__(self, works_endpoint, output_path):
    self.works_endpoint = works_endpoint
    self.output_path = output_path

  def __call__(self, target_filter_pair):
    filter_name, filter_str = target_filter_pair
    output_file = FileSystems.join(self.output_path, filter_name + '.zip')
    save_items_from_endpoint_for_filter_to_zipfile(
      self.works_endpoint, filter_str, output_file
    )

def configure_pipeline(p, opt):
  works_endpoint = get_works_endpoint(opt)
  target_filter_map = get_target_filter_map(works_endpoint, opt)
  target_filter_pairs = sorted(target_filter_map.items())
  _ = (
    p |
    beam.Create(target_filter_pairs) |
    beam.Map(RetrieveAndSaveWorks(works_endpoint, opt.output_path))
  )

def add_main_args(parser):
  source_group = parser.add_argument_group('source')
  source_group.add_argument(
    '--filter', type=str, required=False,
    help='filter to use'
  )
  source_group.add_argument(
    '--filter-name', type=str, required=False,
    help='filter name (within output path)'
  )
  source_group.add_argument(
    '--group-by-published-date', action='store_true', default=False,
    help='group by published date (e.g. year)'
  )
  source_group.add_argument(
    '--max-retries', type=int, default=10,
    help='Number of HTTP request retries.'
  )
  source_group.add_argument(
    '--batch-size', type=int, default=1000,
    help='Number rows per page to retrieve.'
  )
  source_group.add_argument(
    '--email', type=str, required=False,
    help='email to identify the requests as (see Crossref API etiquette)'
  )

  output_group = parser.add_argument_group('output')
  output_group.add_argument(
    '--output-path', required=True,
    help='Output directory to write results to.'
  )
  output_group.add_argument(
    '--compression',
    type=str,
    choices=[DEFLATE, BZIP2, LZMA],
    default=DEFLATE,
    help='Zip compression to use (requires Python 3.3+).'
  )

  parser.add_argument(
    '--debug', action='store_true', default=False,
    help='enable debug output'
  )

def parse_args(argv=None):
  parser = argparse.ArgumentParser(
    description='Download Crossref Works data'
  )
  add_main_args(parser)
  add_cloud_args(parser)

  args = parser.parse_args(argv)

  if args.debug:
    logging.getLogger().setLevel('DEBUG')

  process_cloud_args(
    args, args.output_path,
    name='datacapsule-crossref'
  )

  get_logger().info('args: %s', args)

  return args

def run(argv=None):
  args = parse_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions.from_dictionary(vars(args))
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(args.runner, options=pipeline_options) as p:
    configure_pipeline(p, args)

    # Execute the pipeline and wait until it is completed.


if __name__ == '__main__':
  logging.basicConfig(level='INFO')

  run()
