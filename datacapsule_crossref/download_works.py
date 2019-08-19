from __future__ import absolute_import

import argparse
import logging
import os
import re
import json
import zipfile
from zipfile import ZipFile
from asyncio import Future
from typing import Dict

from urllib3.exceptions import ProtocolError

from six.moves.urllib.parse import urlencode

from requests import Response
from requests_futures.sessions import FuturesSession
from tqdm import tqdm

from datacapsule_crossref.utils.io import makedirs
from datacapsule_crossref.utils.requests import configure_session_retry


DEFLATE = "deflate"
BZIP2 = "bzip2"
LZMA = "lzma"

DEFAULT_CROSSREF_API_URL = 'http://api.crossref.org/works'


LOGGER = logging.getLogger(__name__)


def get_args_parser():
    parser = argparse.ArgumentParser(
        description='Download Crossref Works data'
    )
    parser.add_argument(
        '--base-url', type=str,
        default=DEFAULT_CROSSREF_API_URL,
        help='base url to retrieve works from'
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


def add_url_parameters(base_url, parameters):
    if not parameters:
        return base_url
    if isinstance(parameters, (dict, list)):
        parameters = urlencode(parameters)
    return '{}{}{}'.format(
        base_url, '&' if '?' in base_url else '?', parameters
    )


class PageResponseIterator:
    def __init__(
            self,
            session: FuturesSession,
            base_url: str,
            start_cursor: str = '*'):
        self.session = session
        self.base_url = base_url
        self.current_cursor = start_cursor
        self._next_cursor_pattern = re.compile(r'"next-cursor":\s*"([^"]+?)"')
        self._future_response_by_cursor_map = {}

    def get_cache_size(self) -> int:
        LOGGER.debug('cached cursors: %s', self._future_response_by_cursor_map.keys())
        return len(self._future_response_by_cursor_map)

    def _request_page(self, cursor: str) -> Future:
        LOGGER.debug('requesting page for cursor: %s', cursor)
        url = add_url_parameters(self.base_url, {'cursor': cursor})
        return self.session.get(url, stream=True)

    def _remove_response_from_cache(self, cursor: str):
        try:
            del self._future_response_by_cursor_map[cursor]
        except KeyError:
            pass

    def _get_future_response(self, cursor: str) -> Future:
        future_response = self._future_response_by_cursor_map.get(cursor)
        if not future_response:
            future_response = self._request_page(cursor)
            self._future_response_by_cursor_map[cursor] = future_response
        return future_response

    def _get_response(self, cursor: str) -> Response:
        try:
            response = self._get_future_response(cursor).result()
            response.raise_for_status()
            return response
        except Exception:
            self._remove_response_from_cache(cursor)
            raise

    def _preload_future_response(self, cursor: str):
        self._get_future_response(cursor)

    def __iter__(self):
        while self.current_cursor:
            try:
                response = self._get_response(self.current_cursor)
                self._remove_response_from_cache(self.current_cursor)

                # try to find the next cursor in the first response characters
                # we don't need to wait until the whole response has been received
                raw = response.raw
                raw.decode_content = True
                first_bytes = raw.read(1000)
                first_chars = first_bytes.decode()
                LOGGER.debug('first_chars: %s', first_chars)
                m = self._next_cursor_pattern.search(first_chars)
                next_cursor = m.group(1).replace('\\/', '/') if m else None
                LOGGER.debug('next_cursor: %s', next_cursor)
                if next_cursor == self.current_cursor:
                    next_cursor = None

                if next_cursor:
                    # request the next page as soon as possible,
                    # we will read the result in the next iteration
                    self._preload_future_response(next_cursor)
                else:
                    LOGGER.info('no next_cursor found, end reached?')

                remaining_bytes = raw.read()
                content = first_bytes + remaining_bytes
                yield next_cursor, content

                self.current_cursor = next_cursor
            except ProtocolError as e:
                LOGGER.error('protocol error (%s), retrying', e, exc_info=e)


def iter_page_responses_from_session(*args, **kwargs):
    yield from PageResponseIterator(*args, **kwargs)


def iter_page_responses(base_url, max_retries, start_cursor='*'):
    with FuturesSession(max_workers=10) as session:
        configure_session_retry(
            session=session,
            max_retries=max_retries,
            status_forcelist=[500, 502, 503, 504]
        )

        yield from iter_page_responses_from_session(
            session=session,
            base_url=base_url,
            start_cursor=start_cursor
        )


def save_page_responses(base_url, zip_filename, max_retries, items_per_page, compression):
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

    LOGGER.info('start cursor: %s (offset %s, total: %s)',
                start_cursor, offset, total_results)

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
                LOGGER.debug('response: %s (%s)', len(
                    page_response), next_cursor)

                if total_results is None:
                    m = total_results_pattern.search(page_response.decode())
                    total_results = int(m.group(1)) if m else None

                if pbar is None:
                    pbar = tqdm(total=total_results,
                                leave=False, initial=offset)

                zf.writestr(page_filename_pattern.format(
                    page_index, offset), page_response)

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


def download_works_direct(base_url, zip_filename, batch_size, max_retries, compression, email=None):
    parameters = [
        ('rows', batch_size)
    ]
    if email:
        parameters.append(('mailto', email))
    url = add_url_parameters(base_url, parameters)
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
        args.base_url,
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
