from __future__ import absolute_import

import argparse
import logging
from itertools import groupby
from threading import Thread
from queue import Queue

from tqdm import tqdm
import pandas as pd
import lmdb

from datacapsule_crossref.utils import (
  write_csv,
  iter_with_file_read_progress,
  compression_by_filename
)

from datacapsule_crossref.doi_utils import doi_to_normalised_key

def get_logger():
  return logging.getLogger(__name__)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Cleans DOIs stored in a LMDB database'
  )
  parser.add_argument(
    '--input-file', type=str, required=True,
    help='path to input file'
  )
  parser.add_argument(
    '--lmdb-root', type=str, required=True,
    help='path to lmdb root'
  )
  parser.add_argument(
    '--output-file', type=str, required=True,
    help='path to output file (csv or tsv)'
  )
  parser.add_argument(
    '--delimiter', type=str, required=False,
    help='output file delimiter (otherwise determined by filename)'
  )
  parser.add_argument(
    '--check-sub-dois',
    action='store_true',
    help='check for sub dois (within the trusted keys)'
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
      if pd.isna(cited_doi):
        cited_doi = None
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

def check_sub_dois(env):
  total = env.stat()['entries']

  get_logger().info('checking for sub dois')
  with env.begin(write=False) as txn:
    cursor = txn.cursor()
    prev_key = None
    for key, _ in tqdm(cursor, total=total):
      if prev_key and prev_key.startswith(key):
        get_logger().info('key %s sub key of %s', key, prev_key)
      else:
        prev_key = key

def iter_clean_citations(grouped_citations, env):
  logger = get_logger()

  with env.begin(write=False) as txn:
    with txn.cursor() as other_cursor:
      for citing_doi, cited_dois in grouped_citations:
        if cited_dois:
          for cited_doi in sorted(cited_dois):
            try:
              cited_doi_key = doi_to_normalised_key(cited_doi)
            except:
              raise ValueError('failed t convert: {}'.format(cited_doi))
            encoded_cited_doi_key = cited_doi_key.encode('utf-8')
            if other_cursor.set_range(encoded_cited_doi_key):
              encoded_other_doi_key, other_value = other_cursor.item()
              other_doi_key = encoded_other_doi_key.decode('utf-8')
              other_doi = other_value.decode('utf-8')
              if other_doi_key == cited_doi_key:
                logger.debug('found exact cited_doi: %s', cited_doi)
                yield citing_doi, other_doi, cited_doi, True
              elif other_doi_key and cited_doi_key.startswith(other_doi_key):
                # this should never be true
                logger.debug('found cited_doi: %s starting with %s', cited_doi, other_doi)
                yield citing_doi, other_doi, cited_doi, True
              elif other_cursor.prev():
                encoded_other_doi_key, other_value = other_cursor.item()
                other_doi_key = encoded_other_doi_key.decode('utf-8')
                other_doi = other_value.decode('utf-8')
                if other_doi_key and cited_doi_key.startswith(other_doi_key):
                  logger.debug('found2 cited_doi: %s starting with %s', cited_doi, other_doi)
                  yield citing_doi, other_doi, cited_doi, True
                else:
                  logger.debug('cited_doi appears to be invalid: %s', cited_doi)
                  yield citing_doi, cited_doi, cited_doi, False
            else:
              logger.debug(
                'cited_doi appears to be invalid2: %s (%s)', cited_doi, encoded_cited_doi_key
              )
              yield citing_doi, cited_doi, cited_doi, False
        else:
          yield citing_doi, None, None, None

def read_source_citations_to_queue(input_file, output_queue, num_output_processes, chunksize):
  source_citations = iter_with_file_read_progress(
    input_file,
    lambda fp: iter_grouped_citations(iter_citations(
      fp,
      chunksize=chunksize,
      compression=compression_by_filename(input_file)
    ))
  )
  for item in source_citations:
    output_queue.put(item)
  for _ in range(num_output_processes):
    output_queue.put(None)

def iter_from_queue(q, num_workers=1):
  workers_running = num_workers
  while workers_running > 0:
    item = q.get()
    if item is None:
      workers_running -= 1
      continue
    yield item

def iter_to_queue(iterable, q):
  for item in iterable:
    q.put(item)

def clean_citations_to_queue(input_queue, output_queue, env):
  iter_to_queue(
    iter_clean_citations(iter(input_queue.get, None), env),
    output_queue
  )
  output_queue.put(None)

def make_daemon(p):
  p.daemon = True
  return p

def iter_read_file_and_clean_citations(
  input_file,
  chunksize,
  env):

  Q = Queue
  P = Thread

  num_workers = 5

  source_citations_queue = Q(10 * num_workers)
  cleaned_citations_queue = Q(10 * num_workers)

  for _ in range(num_workers):
    make_daemon(P(
      target=clean_citations_to_queue,
      args=(source_citations_queue, cleaned_citations_queue, env)
    )).start()

  make_daemon(P(
    target=read_source_citations_to_queue,
    args=(input_file, source_citations_queue, num_workers, chunksize)
  )).start()

  yield from iter_from_queue(cleaned_citations_queue, num_workers)

def bool_to_str(b):
  if b is None:
    return ''
  return 'true' if b else 'false'

def clean_and_extract_citations_from_lmdb_store(argv):
  args = get_args_parser().parse_args(argv)

  input_file = args.input_file
  lmdb_root = args.lmdb_root
  output_file = args.output_file
  chunksize = args.chunksize

  get_logger().info('lmdb_root: %s', lmdb_root)
  get_logger().info('output_file: %s', output_file)

  env = lmdb.open(lmdb_root, max_dbs=2, map_size=int(32e9), create=False)
  get_logger().info('len: %s', env.stat())

  if args.check_sub_dois:
    check_sub_dois(env)

  get_logger().info('cleaning citations')

  cleaned_citations = iter_read_file_and_clean_citations(
    input_file,
    chunksize,
    env
  )

  write_csv(
    output_file,
    ['citing_doi', 'cited_doi', 'original_cited_doi', 'cited_doi_valid', 'cited_doi_corrected'],
    (
      (
        citing_doi,
        cited_doi,
        original_cited_doi,
        bool_to_str(cited_doi_valid),
        bool_to_str(cited_doi != original_cited_doi)
      )
      for citing_doi, cited_doi, original_cited_doi, cited_doi_valid in cleaned_citations
    ),
    delimiter=args.delimiter
  )

def main(argv=None):
  clean_and_extract_citations_from_lmdb_store(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
