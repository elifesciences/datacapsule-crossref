from __future__ import absolute_import

import argparse
import logging
import os
import re
import json
import zipfile
from zipfile import ZipFile
from urllib.parse import quote

from requests_futures.sessions import FuturesSession
from tqdm import tqdm

from datacapsule_crossref.utils import (
  makedirs,
  configure_session_retry
)

DEFLATE = "deflate"
BZIP2 = "bzip2"
LZMA = "lzma"

def get_logger():
  return logging.getLogger(__name__)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Download Crossref Works data'
  )
  parser.add_argument(
    '--output-file', type=str, required=True,
    help='path to output file'
  )
  parser.add_argument(
    '--max-retries',
    type=int,
    default=10,
    help='Number of HTTP request retries.'
  )
  parser.add_argument(
    '--batch-size',
    type=int,
    default=1000,
    help='Number rows per page to retrieve.'
  )
  parser.add_argument(
    '--compression',
    type=str,
    choices=[DEFLATE, BZIP2, LZMA],
    default=DEFLATE,
    help='Zip compression to use (requires Python 3.3+).'
  )
  parser.add_argument(
    '--email', type=str, required=False,
    help='email to identify the requests as (see Crossref API etiquette)'
  )
  parser.add_argument(
    '--debug', action='store_true',
    help='Enable debug logging'
  )
  return parser

def iter_page_responses(base_url, max_retries, start_cursor='*'):
  logger = get_logger()

  next_cursor_pattern = re.compile(r'"next-cursor":"([^"]+?)"')

  with FuturesSession(max_workers=10) as session:
    configure_session_retry(
      session=session,
      max_retries=max_retries,
      status_forcelist=[500, 502, 503, 504]
    )

    def request_page(cursor):
      url = '{}{}cursor={}'.format(
        base_url, '&' if '?' in base_url else '?', quote(cursor)
      )
      return session.get(url, stream=True)

    future_response = request_page(start_cursor)
    previous_cursor = start_cursor
    while future_response:
      response = future_response.result()
      response.raise_for_status()

      # try to find the next cursor in the first response characters
      # we don't need to wait until the whole response has been received
      raw = response.raw
      raw.decode_content = True
      first_bytes = raw.read(1000)
      first_chars = first_bytes.decode()
      m = next_cursor_pattern.search(first_chars)
      next_cursor = m.group(1).replace('\\/', '/') if m else None
      logger.debug('next_cursor: %s', next_cursor)
      if next_cursor == previous_cursor:
        next_cursor = None

      if next_cursor:
        # request the next page as soon as possible,
        # we will read the result in the next iteration
        future_response = request_page(next_cursor)
        previous_cursor = next_cursor
      else:
        logger.info('no next_cursor found, end reached?')
        future_response = None

      remaining_bytes = raw.read()
      content = first_bytes + remaining_bytes
      yield next_cursor, content

def save_page_responses(base_url, zip_filename, max_retries, items_per_page, compression):
  logger = get_logger()

  state_filename = zip_filename + '.meta'
  page_filename_pattern = '{}-page-{{}}-offset-{{}}.json'.format(
    os.path.splitext(os.path.basename(zip_filename))[0]
  )

  start_cursor = '*'
  offset = 0
  page_index = 0
  total_results = None
  if os.path.isfile(state_filename):
    with open(state_filename, 'r') as meta_f:
      previous_state = json.load(meta_f)
      start_cursor = previous_state['cursor']
      page_index = previous_state['page_index']
      offset = previous_state['offset']
      total_results = previous_state.get('total_results')
      if previous_state['items_per_page'] != items_per_page:
        raise RuntimeError('please continue using the same items per page: {}'.format(
          previous_state['items_per_page']
        ))

  logger.info('start cursor: %s (offset %s, total: %s)', start_cursor, offset, total_results)

  total_results_pattern = re.compile(r'"total-results":(\d+)\D')

  pbar = None

  try:
    with ZipFile(zip_filename, 'a', compression) as zf:
      page_responses = iter_page_responses(
        base_url,
        max_retries=max_retries,
        start_cursor=start_cursor
      )

      for next_cursor, page_response in page_responses:
        logger.debug('response: %s (%s)', len(page_response), next_cursor)

        if total_results is None:
          m = total_results_pattern.search(page_response.decode())
          total_results = int(m.group(1)) if m else None

        if pbar is None:
          pbar = tqdm(total=total_results, leave=False, initial=offset)

        zf.writestr(page_filename_pattern.format(page_index, offset), page_response)

        page_index += 1
        offset += items_per_page
        pbar.update(items_per_page)

        if next_cursor:
          state_str = json.dumps({
            'cursor': next_cursor,
            'offset': offset,
            'page_index': page_index,
            'items_per_page': items_per_page,
            'total_results': total_results
          })
          with open(state_filename, 'w') as meta_f:
            meta_f.write(state_str)
  finally:
    if pbar:
      pbar.close()

def download_works_direct(zip_filename, batch_size, max_retries, compression, email=None):
  url = 'http://api.crossref.org/works?rows={}'.format(
    batch_size
  )
  if email:
    url += '&mailto=' + quote(email)
  save_page_responses(
    url,
    zip_filename=zip_filename,
    max_retries=max_retries,
    items_per_page=batch_size,
    compression=compression
  )

def download_direct(argv):
  args = get_args_parser().parse_args(argv)

  if args.debug:
    logging.getLogger().setLevel('DEBUG')

  output_file = args.output_file
  makedirs(os.path.dirname(output_file), exist_ok=True)

  compression = zipfile.ZIP_DEFLATED
  if args.compression == BZIP2:
    compression = zipfile.ZIP_BZIP2
  elif args.compression == LZMA:
    compression = zipfile.ZIP_LZMA

  download_works_direct(
    output_file,
    batch_size=args.batch_size,
    max_retries=args.max_retries,
    compression=compression,
    email=args.email
  )

def main(argv=None):
  download_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
