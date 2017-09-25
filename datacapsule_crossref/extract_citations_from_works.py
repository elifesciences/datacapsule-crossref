from __future__ import absolute_import

import argparse
import logging
import os
import json
from zipfile import ZipFile
import codecs

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

def iter_zip_citations(input_file):
  logger = get_logger()

  utf8_reader = codecs.getreader("utf-8")

  with ZipFile(input_file, 'r') as zip_f:
    names = zip_f.namelist()
    logger.info('files: %s', len(names))
    for name in tqdm(names):
      with zip_f.open(name) as zipped_f:
        response = json.load(utf8_reader(zipped_f))
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
