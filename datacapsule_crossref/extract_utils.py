import argparse
import logging
import json
import zipfile

from apache_beam.io.filesystems import FileSystems

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
    with zipfile.ZipFile(zip_f, 'r') as zf:
      filenames = zf.namelist()
      for filename in filenames:
        if filename.endswith('.json'):
          with zf.open(filename) as json_f:
            yield zip_filename + '#' + filename, json.loads(json_f.read())
