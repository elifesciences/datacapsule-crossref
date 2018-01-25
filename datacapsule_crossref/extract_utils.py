import logging
import json
import zipfile
from io import BytesIO

import dateutil

import pandas as pd

from apache_beam.io.filesystems import FileSystems

from datacapsule_crossref.extract_summaries_from_works import (
  Columns as SummaryColumns
)

from datacapsule_crossref.reference_stats import (
  update_counter_with_references,
  Columns as ReferenceStatsColumns
)

from datacapsule_crossref.utils.collection import (
  iter_flatten
)

META_FILE_SUFFIX = '.meta'

def get_logger():
  return logging.getLogger(__name__)

def find_matching_filenames(pattern):
  return (x.path for x in FileSystems.match([pattern])[0].metadata_list)

def find_matching_filenames_for_patterns(patterns):
  return iter_flatten(
    find_matching_filenames(pattern) for pattern in patterns
  )

def find_zip_filenames_with_meta_file(data_path):
  zip_file_patterns = [
    FileSystems.join(data_path, '*.zip'),
    FileSystems.join(data_path, '**/*.zip')
  ]
  meta_file_patterns = [s + META_FILE_SUFFIX for s in zip_file_patterns]
  zip_filenames = sorted(set(find_matching_filenames_for_patterns(zip_file_patterns)))
  meta_filenames = set(find_matching_filenames_for_patterns(meta_file_patterns))
  get_logger().debug('zip_file_patterns: %s', zip_file_patterns)
  get_logger().debug('meta_file_patterns: %s', meta_file_patterns)
  get_logger().debug('zip_filenames: %s', zip_filenames)
  get_logger().debug('meta_filenames: %s', meta_filenames)
  return [
    filename for filename in zip_filenames
    if filename + META_FILE_SUFFIX in meta_filenames
  ]

def read_works_from_zip(zip_filename):
  with FileSystems.open(zip_filename) as zip_f:
    data = zip_f.read()
  with zipfile.ZipFile(BytesIO(data), 'r') as zf:
    filenames = zf.namelist()
    for filename in filenames:
      if filename.endswith('.json'):
        with zf.open(filename) as json_f:
          yield zip_filename + '#' + filename, json.loads(json_f.read())

def extract_year(created):
  return dateutil.parser.parse(created).year

def update_reference_counters_for_summary(typed_counter_with_examples, work_summary):
  doi = work_summary.get(SummaryColumns.DOI)
  key_values = [
    (SummaryColumns.PUBLISHER, work_summary[SummaryColumns.PUBLISHER]),
    (SummaryColumns.CONTAINER_TITLE, work_summary[SummaryColumns.CONTAINER_TITLE]),
    (SummaryColumns.FIRST_SUBJECT_AREA, work_summary[SummaryColumns.FIRST_SUBJECT_AREA]),
    (SummaryColumns.CREATED, extract_year(work_summary[SummaryColumns.CREATED]))
  ]
  for key, value in key_values:
    typed_counter_with_examples.add('total_{}'.format(key), value, doi)
  is_non_oa = (
    work_summary[SummaryColumns.REFERENCE_COUNT] > 0 and
    work_summary[SummaryColumns.HAS_REFERENCES] == 0
  )
  if is_non_oa:
    for key, value in key_values:
      typed_counter_with_examples.add('non_oa_ref_{}'.format(key), value, doi)
  debug_json = work_summary[SummaryColumns.DEBUG]
  if debug_json:
    update_counter_with_references(
      typed_counter_with_examples,
      doi,
      json.loads(debug_json)
    )
  return typed_counter_with_examples

def typed_counter_with_examples_to_dict(typed_counter_with_examples):
  for counter_type, key, count, examples in typed_counter_with_examples:
    yield {
      ReferenceStatsColumns.TYPE: counter_type,
      ReferenceStatsColumns.KEY: key,
      ReferenceStatsColumns.COUNT: count,
      ReferenceStatsColumns.EXAMPLES: examples
    }

def dict_list_to_dataframe(dict_list, columns=None):
  return pd.DataFrame.from_records(dict_list, columns=columns)
