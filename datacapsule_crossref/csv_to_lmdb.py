from __future__ import absolute_import

import argparse
import logging
import os
from shutil import rmtree
from itertools import groupby
import pickle

import pandas as pd
import lmdb

from datacapsule_crossref.utils import (
  compression_by_filename,
  iter_with_file_read_progress
)

from datacapsule_crossref.doi_utils import (
  doi_to_normalised_key
)

def get_logger():
  return logging.getLogger(__name__)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Convert Crossref Citations Csv to a LMDB database'
  )
  parser.add_argument(
    '--input-file', type=str, required=True,
    help='path to input file'
  )
  parser.add_argument(
    '--output-root', type=str, required=True,
    help='output root path of the LMDB database'
  )
  parser.add_argument(
    '--chunksize', type=int, default=1000 * 1000,
    help='chunk size to use (should be small enough to fit into memory, '
      'big enough to reduce overhead)'
  )
  return parser

def doi_to_publisher_id(doi):
  return doi[:doi.find('/')]

def iter_citations(fp, chunksize, compression):
  df_chunks = pd.read_csv(
    fp,
    sep='\t',
    chunksize=chunksize,
    compression=compression,
    dtype='str'
  )
  for df in df_chunks:
    for citing_doi, cited_doi in zip(df['citing_doi'].values, df['cited_doi'].values):
      yield citing_doi, cited_doi

def iter_grouped_citations(citations):
  key = lambda citation: citation[0]
  for citing_doi, grouped_citations in groupby(citations, key=key):
    yield citing_doi, [d for _, d in grouped_citations if d]

def iter_batch(coll, batch_size):
  batch = []
  for x in coll:
    batch.append(x)
    if len(batch) >= batch_size:
      yield batch
      batch = []
  if batch:
    yield batch

def force_string(s):
  if s is None:
    return ''
  if not isinstance(s, str):
    get_logger().info('not a string: %s', s)
    return str(s)
  return s

def convert_csv_to_lmdb(argv):
  args = get_args_parser().parse_args(argv)

  input_file = args.input_file
  output_root = args.output_root

  get_logger().info('input_file: %s', input_file)
  get_logger().info('output_root: %s', output_root)

  chunksize = args.chunksize
  batch_size = 1000

  if os.path.isdir(output_root):
    rmtree(output_root)

  env = lmdb.open(output_root, map_size=int(42e9), writemap=True)

  with env.begin(write=True) as txn:
    with txn.cursor() as cursor:
      citation_batches = iter_batch(
        iter_with_file_read_progress(
          input_file,
          lambda fp: iter_grouped_citations(iter_citations(
            fp,
            chunksize=chunksize,
            compression=compression_by_filename(input_file)
          ))
        ),
        batch_size
      )
      for citation_batch in citation_batches:
        cursor.putmulti(
          (
            (
              doi_to_normalised_key(force_string(citing_doi)).encode('utf8'),
              force_string(citing_doi).encode('utf8'),
              # pickle.dumps({
              #   'doi': citing_doi,
              #   'cited_dois': cited_dois
              # })
            )
            for citing_doi, cited_dois in citation_batch
          ),
          overwrite=False
        )

def main(argv=None):
  convert_csv_to_lmdb(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
