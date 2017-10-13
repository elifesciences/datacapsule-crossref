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
  write_csv,
  iter_dict_to_list
)

from datacapsule_crossref.doi_utils import clean_doi

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
    '--provenance', required=False,
    action='store_true',
    help='include provenance information (i.e. source filename)'
  )
  parser.add_argument(
    '--no-clean-dois', required=False,
    action='store_true',
    help='whether to disable DOI cleaning'
  )
  parser.add_argument(
    '--empty-link', required=False,
    action='store_true',
    help='whether to include an empty link where no citations are available'
  )
  parser.add_argument(
    '--debug', required=False,
    action='store_true',
    help='whether to include debug information (implies empty-link)'
  )
  return parser

class Columns(object):
  DOI = 'citing_doi'
  CITATION_DOIS = 'citation_dois'
  CITED_DOI = 'cited_doi'
  HAS_REFERENCES = 'has_references'
  NUM_REFERENCES = 'num_references'
  NUM_CITATIONS_WITHOUT_DOI = 'num_citations_without_doi'
  NUM_DUPLICATE_CITATION_DOIS = 'num_duplicate_citation_dois'
  DEBUG = 'debug'
  PROVENANCE = 'provenance'


REGULAR_COLUMNS = [
  Columns.DOI,
  Columns.CITED_DOI,
  Columns.HAS_REFERENCES,
  Columns.NUM_REFERENCES,
  Columns.NUM_CITATIONS_WITHOUT_DOI,
  Columns.NUM_DUPLICATE_CITATION_DOIS
]

def extract_doi_from_reference(reference):
  doi = reference.get('DOI')
  if not doi:
    get_logger().debug('doi not found in reference: %s', reference)
  return doi

def extract_citations_from_work(work, doi_filter):
  doi = doi_filter(work.get('DOI'))
  has_reference = 'reference' in work
  references = work.get('reference', [])
  raw_citation_dois = [r.get('DOI') for r in references]
  filtered_dois = [doi_filter(doi) for doi in raw_citation_dois]
  non_empty_cleaned_citation_dois = [doi for doi in filtered_dois if doi]
  references_without_dois = [r for doi, r in zip(filtered_dois, references) if not doi]
  num_citations_without_doi = len(raw_citation_dois) - len(non_empty_cleaned_citation_dois)
  unique_citation_dois = sorted(set(non_empty_cleaned_citation_dois))
  num_duplicate_citation_dois = len(non_empty_cleaned_citation_dois) - len(unique_citation_dois)
  return {
    Columns.DOI: doi,
    Columns.CITATION_DOIS: unique_citation_dois,
    Columns.HAS_REFERENCES: 1 if has_reference else 0,
    Columns.NUM_REFERENCES: len(references),
    Columns.NUM_CITATIONS_WITHOUT_DOI: num_citations_without_doi,
    Columns.NUM_DUPLICATE_CITATION_DOIS: num_duplicate_citation_dois,
    Columns.DEBUG: json.dumps(references_without_dois)
  }

def extract_citations_from_response(response, clean_doi_enabled):
  message = response.get('message', {})
  items = message.get('items', [])
  doi_filter = clean_doi if clean_doi_enabled else lambda x: x
  for work in items:
    yield extract_citations_from_work(work, doi_filter)

def read_zip_to_queue(input_file, output_queue, num_output_processes):
  with ZipFile(input_file, 'r') as zip_f:
    names = zip_f.namelist()
    get_logger().info('files: %s', len(names))
    for name in tqdm(names, smoothing=0.1):
      output_queue.put((name, zip_f.read(name)))
  for _ in range(num_output_processes):
    output_queue.put(None)

def extract_citations_to_queue(input_queue, output_queue, clean_doi_enabled):
  for name, item in iter(input_queue.get, None):
    response = json.loads(item.decode('utf-8'))
    output_queue.put([
      {
        **extracted,
        Columns.PROVENANCE: name
      }
      for extracted in extract_citations_from_response(response, clean_doi_enabled)
    ])
  output_queue.put(None)

def make_daemon(p):
  p.daemon = True
  return p

def iter_zip_citations(
  input_file, num_workers=None, multi_processing=None, clean_doi_enabled=False):

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
      args=(response_queue, citations_queue, clean_doi_enabled)
    )).start()

  make_daemon(P(
    target=read_zip_to_queue,
    args=(input_file, response_queue, num_workers)
  )).start()

  workers_running = num_workers
  while workers_running > 0:
    batched_item = citations_queue.get()
    if batched_item is None:
      workers_running -= 1
      continue
    for item in batched_item:
      yield item

def flatten_citations(citations, empty_link):
  for item in citations:
    if empty_link:
      # won't have the cited_doi but other stats
      yield item
    for cited_doi in item.get(Columns.CITATION_DOIS):
      yield {
        Columns.DOI: item.get(Columns.DOI),
        Columns.PROVENANCE: item.get(Columns.PROVENANCE),
        Columns.CITED_DOI: cited_doi
      }

def extract_citations_from_works_direct(argv):
  args = get_args_parser().parse_args(argv)

  output_file = args.output_file
  makedirs(os.path.basename(output_file), exist_ok=True)

  get_logger().info('output_file: %s', output_file)

  flattened_citations = flatten_citations(
    iter_zip_citations(
      args.input_file,
      num_workers=args.num_workers,
      multi_processing=args.multi_processing,
      clean_doi_enabled=not args.no_clean_dois
    ),
    empty_link=args.empty_link or args.debug
  )

  columns = REGULAR_COLUMNS.copy()

  if args.provenance:
    columns += [Columns.PROVENANCE]

  if args.debug:
    columns += [Columns.DEBUG]

  write_csv(
    output_file,
    columns,
    iter_dict_to_list(flattened_citations, columns),
    delimiter=args.delimiter
  )

def main(argv=None):
  extract_citations_from_works_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
