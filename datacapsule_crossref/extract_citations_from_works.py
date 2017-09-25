from __future__ import absolute_import

import argparse
import logging
import os
import json
from zipfile import ZipFile
import codecs
import multiprocessing as mp

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
  return parser

def clean_doi(doi):
  return doi.strip().replace('\n', ' ').replace('\t', ' ') if doi else None

def extract_citations_from_work(work):
  doi = clean_doi(work.get('DOI'))
  references = work.get('reference', [])
  citation_dois = [r.get('DOI') for r in references]
  citation_dois = [clean_doi(doi) for doi in citation_dois if doi]
  return doi, citation_dois

def extract_citations_from_response(response):
  message = response.get('message', {})
  items = message.get('items', [])
  for work in items:
    yield extract_citations_from_work(work)

def read_zip_to_queue(input_file, queue):
  with ZipFile(input_file, 'r') as zip_f:
    names = zip_f.namelist()
    get_logger().info('files: %s', len(names))
    for name in tqdm(names):
      queue.put(zip_f.read(name))
  queue.put(None)

def iter_zip_citations(input_file):
  response_queue = mp.Queue(10)
  zip_process = mp.Process(target=read_zip_to_queue, args=(input_file, response_queue))
  zip_process.start()

  while True:
    response = response_queue.get()
    if response is None:
      break
    response = json.loads(response.decode('utf-8'))
    for doi, citation_dois in extract_citations_from_response(response):
      if doi:
        yield doi, citation_dois

def flatten_citations(citations):
  for doi, citation_dois in citations:
    for cited_doi in citation_dois:
      yield doi, cited_doi

def extract_citations_from_works_direct(argv):
  args = get_args_parser().parse_args(argv)

  output_file = args.output_file
  makedirs(os.path.basename(output_file), exist_ok=True)

  write_csv(
    output_file,
    ['citing_doi', 'cited_doi'],
    flatten_citations(
      iter_zip_citations(args.input_file)
    ),
    delimiter=args.delimiter
  )

def main(argv=None):
  extract_citations_from_works_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
