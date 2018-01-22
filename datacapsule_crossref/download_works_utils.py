import json
import logging
from datetime import datetime
from zipfile import ZipFile

from apache_beam.io.filesystems import FileSystems

PRE_X_THRESHOLD = 1800
PRE_X_KEY = 'pre-%d' % PRE_X_THRESHOLD
PRE_X_FILTER = 'until-pub-date:%d' % (PRE_X_THRESHOLD - 1)
PRE_1800_KEY = PRE_X_KEY

MONTHLY_THRESHOLD = 1950

CURRENT_KEY = 'current'

def get_logger():
  return logging.getLogger(__name__)

def get_published_year_counts(works_endpoint):
  return works_endpoint.facet('published')['published']['values']

def add_monthly_filter(filters_by_target, year, month):
  year_month = '%d-%02d' % (year, month)
  month_filter = 'from-pub-date:%s,until-pub-date:%s' % (year_month, year_month)
  filters_by_target['%s/%02d' % (year, month)] = month_filter

def group_year_counts_to_filters_by_target(year_counts, current_date=None):
  filters_by_target = dict()
  pre_x_count = 0
  current_year = current_date.year if current_date else None
  current_month = current_date.month if current_date else None
  for k, v in year_counts.items():
    year = int(k)
    if year < PRE_X_THRESHOLD:
      pre_x_count += v
    elif year < MONTHLY_THRESHOLD:
      filters_by_target[str(k)] = 'from-pub-date:%d,until-pub-date:%d' % (year, year)
    elif current_year and year >= current_year:
      for month in range(1, current_month):
        add_monthly_filter(filters_by_target, year, month)
      filters_by_target[CURRENT_KEY] = 'from-pub-date:%d-%02d' % (current_year, current_month)
    else:
      for month in range(1, 13):
        add_monthly_filter(filters_by_target, year, month)
  if pre_x_count:
    filters_by_target[PRE_X_KEY] = PRE_X_FILTER
  return filters_by_target

def filename_for_doi(doi, ext='.json'):
  return '%s%s' % (doi, ext)

def filename_for_item(item):
  return filename_for_doi(item['DOI'])

def save_item_to_zipfile(item, zf):
  filename = filename_for_item(item)
  data = json.dumps(item)
  zf.writestr(filename, data)

def save_items_to_zipfile(items, zf):
  count = 0
  for item in items:
    save_item_to_zipfile(item, zf)
    count += 1
  return count

def parse_filter_to_dict(filter_str):
  if not filter_str:
    return dict()
  filter_pairs = [
    single_filter.split(':')
    for single_filter in filter_str.split(',')
  ]
  assert all(len(x) == 2 for x in filter_pairs)
  return {
    k: v for k, v in filter_pairs
  }

def get_works_endpoint_with_filter(works_endpoint, filter_str):
  return works_endpoint.filter(**parse_filter_to_dict(filter_str))

def dirname(path):
  return FileSystems.split(path)[0]

def mkdirs_exists_ok(path):
  try:
    FileSystems.mkdirs(path)
  except IOError:
    pass

def delete_ignore_errors(path):
  try:
    FileSystems.delete([path])
  except IOError:
    pass

def load_meta_or_none(meta_filename):
  try:
    with FileSystems.open(meta_filename) as meta_f:
      meta_obj = json.loads(meta_f.read())
      return meta_obj
  except IOError:
    pass

def save_meta(meta_filename, meta_obj):
  with FileSystems.create(meta_filename) as meta_f:
    meta_f.write(json.dumps(meta_obj))

def is_already_download(filter_str, output_file, meta_filename):
  if not FileSystems.exists(output_file):
    return False
  meta_obj = load_meta_or_none(meta_filename)
  if meta_obj:
    get_logger().info('existing meta: %s', meta_obj)
    if meta_obj['filter'] == filter_str:
      return True
  return False

def save_items_from_endpoint_for_filter_to_zipfile(works_endpoint, filter_str, output_file):
  items = get_works_endpoint_with_filter(works_endpoint, filter_str)
  mkdirs_exists_ok(dirname(output_file))
  meta_filename = output_file + '.meta'
  if is_already_download(filter_str, output_file, meta_filename):
    get_logger().info('already downloaded: %s (%s)', meta_filename, filter_str)
    return
  meta_obj = {
    'download-started': datetime.now().isoformat(),
    'filter': filter_str
  }
  get_logger().info('creating %s', output_file)
  with FileSystems.create(output_file) as output_f:
    with ZipFile(output_f, 'w') as zf:
      count = save_items_to_zipfile(items, zf)
  meta_obj['download-finished'] = datetime.now().isoformat()
  meta_obj['count'] = count
  save_meta(meta_filename, meta_obj)
