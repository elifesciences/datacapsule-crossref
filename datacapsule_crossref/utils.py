import os
import errno
import csv

import six
import requests
from requests.packages.urllib3 import Retry
from tqdm import tqdm

TEMP_FILE_SUFFIX = '.part'
GZIP_COMPRESSION = 'gzip'

def makedirs(path, exist_ok=False):
  try:
    # Python 3
    os.makedirs(path, exist_ok=exist_ok)
  except TypeError:
    # Python 2
    try:
      os.makedirs(path)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise

def configure_session_retry(
  session=None, max_retries=3, backoff_factor=1, status_forcelist=None,
  **kwargs):

  retry = Retry(
    connect=max_retries,
    read=max_retries,
    status_forcelist=status_forcelist,
    redirect=5,
    backoff_factor=backoff_factor
  )
  session.mount('http://', requests.adapters.HTTPAdapter(max_retries=retry, **kwargs))
  session.mount('https://', requests.adapters.HTTPAdapter(max_retries=retry, **kwargs))

def gzip_open(filename, mode):
  import gzip

  if mode == 'w' and not six.PY2:
    from io import TextIOWrapper

    return TextIOWrapper(gzip.open(filename, mode))
  else:
    return gzip.open(filename, mode)

def compression_by_filename(filename):
  if filename.endswith('.gz') or filename.endswith('.gz' + TEMP_FILE_SUFFIX):
    return GZIP_COMPRESSION
  else:
    return None

def optionally_compressed_open(filename, mode):
  if compression_by_filename(filename) == GZIP_COMPRESSION:
    return gzip_open(filename, mode)
  else:
    return open(filename, mode)

def csv_delimiter_by_filename(filename):
  if '.tsv' in filename:
    return '\t'
  else:
    return ','

def iter_read_csv_columns(filename, columns, delimiter=None):
  if delimiter is None:
    delimiter = csv_delimiter_by_filename(filename)
  with optionally_compressed_open(filename, 'r') as csv_f:
    csv_reader = csv.reader(csv_f, delimiter=delimiter)
    first_row = True
    indices = None
    for row in csv_reader:
      if first_row:
        indices = [row.index(c) for c in columns]
        first_row = False
      else:
        yield [row[i] for i in indices]

def iter_with_file_read_progress(filename, generator):
  with open(filename, 'rb') as fp:
    fp.seek(0, os.SEEK_END)
    total = fp.tell()
    fp.seek(0)
    previous_offset = 0
    with tqdm(unit='B', leave=False, total=total, unit_scale=1024 * 1024) as pbar:
      for item in generator(fp):
        current_offset = fp.tell()
        if current_offset != previous_offset:
          pbar.update(current_offset - previous_offset)
        previous_offset = current_offset
        yield item

def open_csv_output(filename):
  return optionally_compressed_open(filename, 'w')

def write_csv_rows(writer, iterable):
  if six.PY2:
    for row in iterable:
      writer.writerow([
        x.encode('utf-8') if isinstance(x, six.text_type) else x
        for x in row
      ])
  else:
    for row in iterable:
      writer.writerow(row)

def write_csv_row(writer, row):
  write_csv_rows(writer, [row])

def write_csv(filename, columns, iterable, delimiter=None):
  if delimiter is None:
    delimiter = csv_delimiter_by_filename(filename)
  temp_filename = filename + TEMP_FILE_SUFFIX
  if os.path.isfile(filename):
    os.remove(filename)
  with open_csv_output(temp_filename) as csv_f:
    writer = csv.writer(csv_f, delimiter=delimiter)
    write_csv_rows(writer, [columns])
    write_csv_rows(writer, iterable)
  os.rename(temp_filename, filename)
