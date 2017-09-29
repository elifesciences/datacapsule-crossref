from __future__ import absolute_import

import argparse
import logging
import os
import json
from zipfile import ZipFile
import multiprocessing as mp
from threading import Thread
from queue import Queue

from tqdm import tqdm

from datacapsule_crossref.utils import (
  makedirs,
  write_csv
)

def get_logger():
  return logging.getLogger(__name__)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Extract Crossref Citations from Works data'
  )
  parser.add_argument(
    '--input-file', type=str, required=True,
    help='path to input file'
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
    '--num-workers', type=int, required=False,
    help='number of workers to use'
  )
  parser.add_argument(
    '--multi-processing', required=False,
    action='store_true',
    help='specify this flag, to enable multi processing'
  )
  parser.add_argument(
    '--provenance', required=True,
    action='store_true',
    help='include provenance information (i.e. source filename)'
  )
  return parser

def clean_doi(doi):
  return doi.strip().replace('\n', ' ').replace('\t', ' ') if doi else None

def extract_citations_from_work(work):
  doi = clean_doi(work.get('DOI'))
  references = work.get('reference', [])
  citation_dois = [r.get('DOI') for r in references]
  citation_dois = sorted(set([clean_doi(doi) for doi in citation_dois if doi]))
  return doi, citation_dois

def extract_citations_from_response(response):
  message = response.get('message', {})
  items = message.get('items', [])
  for work in items:
    yield extract_citations_from_work(work)

def read_zip_to_queue(input_file, output_queue, num_output_processes):
  with ZipFile(input_file, 'r') as zip_f:
    names = zip_f.namelist()
    get_logger().info('files: %s', len(names))
    for name in tqdm(names, smoothing=0.1):
      output_queue.put((name, zip_f.read(name)))
  for _ in range(num_output_processes):
    output_queue.put(None)

def extract_citations_to_queue(input_queue, output_queue):
  for name, item in iter(input_queue.get, None):
    response = json.loads(item.decode('utf-8'))
    output_queue.put([
      (name, doi, citation_dois)
      for doi, citation_dois in extract_citations_from_response(response)
    ])
  output_queue.put(None)

def make_daemon(p):
  p.daemon = True
  return p

def iter_zip_citations(input_file, num_workers=None, multi_processing=None):
  cpu_count = mp.cpu_count()
  if num_workers is None:
    # substract 2: one for the producer and one for the main (output)
    num_workers = max(1, cpu_count - 2)

  if not multi_processing:
    Q = Queue
    P = Thread
  else:
    Q = mp.Queue
    P = mp.Process

  get_logger().info(
    'num_workers: %d (cpu count: %d, mp: %s)', num_workers, cpu_count, multi_processing
  )

  response_queue = Q(10 * num_workers)
  citations_queue = Q(10 * num_workers)

  for _ in range(num_workers):
    make_daemon(P(
      target=extract_citations_to_queue,
      args=(response_queue, citations_queue)
    )).start()

  make_daemon(P(
    target=read_zip_to_queue,
    args=(input_file, response_queue, num_workers)
  )).start()

  workers_running = num_workers
  while workers_running > 0:
    item = citations_queue.get()
    if item is None:
      workers_running -= 1
      continue
    for name, doi, citation_dois in item:
      if doi:
        yield name, doi, citation_dois

def flatten_citations(citations):
  for name, doi, citation_dois in citations:
    for cited_doi in citation_dois:
      yield name, doi, cited_doi

def extract_citations_from_works_direct(argv):
  args = get_args_parser().parse_args(argv)

  output_file = args.output_file
  makedirs(os.path.basename(output_file), exist_ok=True)

  get_logger().info('output_file: %s', output_file)

  flattened_citations = flatten_citations(
    iter_zip_citations(
      args.input_file,
      num_workers=args.num_workers,
      multi_processing=args.multi_processing
    )
  )

  if args.provenance:
    write_csv(
      output_file,
      ['citing_doi', 'cited_doi', 'provenance'],
      ((citing_doi, cited_doi, name) for name, citing_doi, cited_doi in flattened_citations),
      delimiter=args.delimiter
    )
  else:
    write_csv(
      output_file,
      ['citing_doi', 'cited_doi'],
      ((citing_doi, cited_doi) for _, citing_doi, cited_doi in flattened_citations),
      delimiter=args.delimiter
    )

def main(argv=None):
  extract_citations_from_works_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
