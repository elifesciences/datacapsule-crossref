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
from six import string_types
from future.utils import raise_from

from datacapsule_crossref.utils.io import makedirs
from datacapsule_crossref.utils.csv import write_csv
from datacapsule_crossref.utils.collection import iter_sort_window

from datacapsule_crossref.doi_utils import clean_doi


def get_logger():
    return logging.getLogger(__name__)


def get_args_parser():
    parser = argparse.ArgumentParser(
        description='Extract Crossref Summary from Works data'
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
        '--clean-window-size', type=int, default=100 * 1000,
        help='number of records to look back to remove duplicates'
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
        '--debug', required=False,
        action='store_true',
        help='whether to include debug information'
    )
    return parser


def clean_text(s):
    return s.replace('\n', ' ').replace('\t', ' ').replace('  ', ' ').strip() if s else s


def list_to_text(l):
    return ' '.join(l) if isinstance(l, list) else l


class Columns(object):
    DOI = 'doi'
    TITLE = 'title'
    REFERENCE_COUNT = 'reference_count'
    REFERENCED_BY_COUNT = 'referenced_by_count'
    CREATED = 'created'
    TYPE = 'type'
    PUBLISHER = 'publisher'
    CONTAINER_TITLE = 'container_title'
    AUTHOR_COUNT = 'author_count'
    FIRST_SUBJECT_AREA = 'first_subject_area'
    SUBJECT_AREAS = 'subject_areas'

    HAS_REFERENCES = 'has_references'
    NUM_REFERENCES = 'num_references'
    NUM_CITATIONS_WITHOUT_DOI = 'num_citations_without_doi'
    NUM_DUPLICATE_CITATION_DOIS = 'num_duplicate_citation_dois'
    CITED_DOIS = 'cited_dois'

    DEBUG = 'debug'
    PROVENANCE = 'provenance'


SUMMARY_COLUMNS = [
    Columns.DOI,
    Columns.TITLE,
    Columns.REFERENCE_COUNT,
    Columns.REFERENCED_BY_COUNT,
    Columns.CREATED,
    Columns.TYPE,
    Columns.PUBLISHER,
    Columns.CONTAINER_TITLE,
    Columns.AUTHOR_COUNT,
    Columns.FIRST_SUBJECT_AREA,
    Columns.SUBJECT_AREAS,
    Columns.HAS_REFERENCES,
    Columns.NUM_REFERENCES,
    Columns.NUM_CITATIONS_WITHOUT_DOI,
    Columns.NUM_DUPLICATE_CITATION_DOIS,
    Columns.CITED_DOIS
]

# def join_escape(l, sep):
#   if not l or len(l) < 2:
#     return l
#   escaped_sep = '\\' + sep
#   return sep.join([
#     x.replace(sep, escaped_sep)
#     for x in l
#   ])


def extract_summary_from_work(work, doi_filter):
    doi = doi_filter(work.get('DOI'))
    if not isinstance(doi, string_types):
        raise ValueError('work is missing doi: {}'.format(work))
    subject_areas = work.get('subject', [])
    first_subject_area = subject_areas[0] if subject_areas else None
    authors = work.get('author')
    author_count = len(authors) if authors is not None else None

    has_reference = 'reference' in work
    references = work.get('reference', [])
    # Note: in the absence of a reference DOI,
    #   the key may look like a DOI but is a reference to the document itself
    raw_citation_dois = [r.get('DOI') for r in references]
    filtered_dois = [doi_filter(doi) for doi in raw_citation_dois]
    non_empty_cleaned_citation_dois = [doi for doi in filtered_dois if doi]
    references_without_dois = [r for doi, r in zip(
        filtered_dois, references) if not doi]
    num_citations_without_doi = len(
        raw_citation_dois) - len(non_empty_cleaned_citation_dois)
    unique_citation_dois = sorted(set(non_empty_cleaned_citation_dois))
    num_duplicate_citation_dois = len(
        non_empty_cleaned_citation_dois) - len(unique_citation_dois)

    return {
        Columns.DOI: doi,
        Columns.TITLE: clean_text(list_to_text(work.get('title'))),
        Columns.REFERENCE_COUNT: work.get('reference-count'),
        Columns.REFERENCED_BY_COUNT: work.get('is-referenced-by-count'),
        Columns.CREATED: work.get('created', {}).get('date-time'),
        Columns.TYPE: work.get('type'),
        Columns.PUBLISHER: clean_text(list_to_text(work.get('publisher'))),
        Columns.CONTAINER_TITLE: clean_text(list_to_text(work.get('container-title'))),
        Columns.AUTHOR_COUNT: author_count,
        Columns.FIRST_SUBJECT_AREA: first_subject_area,
        Columns.SUBJECT_AREAS: '|'.join(subject_areas),
        Columns.HAS_REFERENCES: 1 if has_reference else 0,
        Columns.NUM_REFERENCES: len(references),
        Columns.NUM_CITATIONS_WITHOUT_DOI: num_citations_without_doi,
        Columns.NUM_DUPLICATE_CITATION_DOIS: num_duplicate_citation_dois,
        Columns.CITED_DOIS: json.dumps(unique_citation_dois),
        Columns.DEBUG: json.dumps(references_without_dois)
    }


def extract_summaries_from_response(response, clean_doi_enabled):
    message = response.get('message', {})
    items = message.get('items', [])
    doi_filter = clean_doi if clean_doi_enabled else lambda x: x
    for i, work in enumerate(items):
        try:
            yield extract_summary_from_work(work, doi_filter)
        except Exception as e:  # pylint: disable=broad-except
            raise_from(RuntimeError('failed to process work {}'.format(i)), e)


def read_zip_to_queue(input_file, output_queue, num_output_processes):
    with ZipFile(input_file, 'r') as zip_f:
        names = zip_f.namelist()
        get_logger().info('files: %s', len(names))
        for name in tqdm(names, smoothing=0.1):
            output_queue.put((name, zip_f.read(name)))
    for _ in range(num_output_processes):
        output_queue.put(None)


def extract_summaries_to_queue(input_queue, output_queue, clean_doi_enabled):
    for name, item in iter(input_queue.get, None):
        try:
            response = json.loads(item.decode('utf-8'))
            output_queue.put([
                {
                    **summary,
                    Columns.PROVENANCE: name
                }
                for summary in extract_summaries_from_response(response, clean_doi_enabled)
            ])
        except Exception as e:  # pylint: disable=broad-except
            raise_from(RuntimeError('failed to process: {}'.format(name)), e)
    output_queue.put(None)


def make_daemon(p):
    p.daemon = True
    return p


def iter_zip_summaries(
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
    summaries_queue = Q(10 * num_workers)

    for _ in range(num_workers):
        make_daemon(P(
            target=extract_summaries_to_queue,
            args=(response_queue, summaries_queue, clean_doi_enabled)
        )).start()

    make_daemon(P(
        target=read_zip_to_queue,
        args=(input_file, response_queue, num_workers)
    )).start()

    workers_running = num_workers
    while workers_running > 0:
        item = summaries_queue.get()
        if item is None:
            workers_running -= 1
            continue
        for d in item:
            if d:
                yield d


def extract_summaries_from_works_direct(argv):
    args = get_args_parser().parse_args(argv)

    output_file = args.output_file
    makedirs(os.path.dirname(output_file), exist_ok=True)

    get_logger().info('output_file: %s', output_file)

    summaries = iter_sort_window(
        iter_zip_summaries(
            args.input_file,
            num_workers=args.num_workers,
            multi_processing=args.multi_processing,
            clean_doi_enabled=not args.no_clean_dois
        ),
        args.clean_window_size,
        key=lambda d: d.get(Columns.DOI),
        on_dropped_item=lambda item: get_logger().info(
            'duplicate doi: %s', item.get(Columns.DOI))
    )

    columns = SUMMARY_COLUMNS.copy()
    if args.provenance:
        columns += [Columns.PROVENANCE]
    if args.debug:
        columns += [Columns.DEBUG]

    write_csv(
        output_file,
        columns,
        (
            [d.get(c) for c in columns]
            for d in summaries
        ),
        delimiter=args.delimiter
    )


def main(argv=None):
    extract_summaries_from_works_direct(argv)


if __name__ == "__main__":
    logging.basicConfig(level='INFO')

    main()
