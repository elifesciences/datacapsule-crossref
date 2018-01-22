from datetime import date
from zipfile import ZIP_DEFLATED
from mock import patch, MagicMock

from crossref.restful import Works

import datacapsule_crossref.download_works_utils as download_works_utils
from datacapsule_crossref.download_works_utils import (
  get_published_year_counts,
  group_year_counts_to_filters_by_target,
  parse_filter_to_dict,
  get_works_endpoint_with_filter,
  is_already_download,
  save_items_from_endpoint_for_filter_to_zipfile,
  PRE_1800_KEY,
  CURRENT_KEY
)

API_URL = 'http://crossref/api'
FILTER_STR_1 = 'filer1:value1'
OUTPUT_FILE_1 = 'output_file.zip'

class TestGetPublishedYearsCounts(object):
  def test_should_call_endpoint_facet(self):
    endpoint = MagicMock(spec=Works)
    assert get_published_year_counts(endpoint) == endpoint.facet.return_value['published']['values']
    endpoint.facet.assert_called_with('published', '*')

class TestGroupYearCountsToFiltersByTarget(object):
  def test_should_return_empty_dict_for_empty_year_counts(self):
    assert group_year_counts_to_filters_by_target(dict()) == dict()

  def test_should_return_pre_1800_for_years_before_1800(self):
    year_counts = {
      "1700": 10
    }
    assert group_year_counts_to_filters_by_target(year_counts) == {
      PRE_1800_KEY: 'until-pub-date:1799'
    }

  def test_should_return_year_filter_for_1800(self):
    year_counts = {
      "1800": 10
    }
    assert group_year_counts_to_filters_by_target(year_counts) == {
      "1800": 'from-pub-date:1800,until-pub-date:1800'
    }

  def test_should_return_year_filter_for_1949(self):
    year_counts = {
      "1949": 10
    }
    assert group_year_counts_to_filters_by_target(year_counts) == {
      "1949": 'from-pub-date:1949,until-pub-date:1949'
    }

  def test_should_return_monthly_filter_for_1950(self):
    year_counts = {
      "1950": 10
    }
    assert group_year_counts_to_filters_by_target(year_counts) == {
      "1950/%02d" % m: 'from-pub-date:1950-%02d,until-pub-date:1950-%02d' % (m, m)
      for m in range(1, 13)
    }

  def test_should_return_current_for_future_years(self):
    year_counts = {
      "3000": 10
    }
    current_date = date(2000, 1, 1)
    assert group_year_counts_to_filters_by_target(year_counts, current_date=current_date) == {
      CURRENT_KEY: 'from-pub-date:2000-01'
    }

  def test_should_return_monthly_filter_in_current_year_before_current_month(self):
    year_counts = {
      "2000": 10
    }
    current_date = date(2000, 3, 1)
    assert group_year_counts_to_filters_by_target(year_counts, current_date=current_date) == {
      "2000/01": 'from-pub-date:2000-01,until-pub-date:2000-01',
      "2000/02": 'from-pub-date:2000-02,until-pub-date:2000-02',
      CURRENT_KEY: 'from-pub-date:2000-03'
    }

class TestParseFilterToDict(object):
  def test_should_rempty_dict_if_filter_is_empty(self):
    assert parse_filter_to_dict('') == dict()

  def test_should_parse_single_filter(self):
    assert parse_filter_to_dict('filter1:value1') == {
      'filter1': 'value1'
    }

  def test_should_parse_multiple_filters(self):
    assert parse_filter_to_dict('filter1:value1,filter2:value2') == {
      'filter1': 'value1',
      'filter2': 'value2'
    }

class TestGetWorksEndpointWithFilter(object):
  def test_should_pass_filter_to_filter_function(self):
    works_endpoint = MagicMock(spec=Works)
    assert (
      get_works_endpoint_with_filter(works_endpoint, 'filter1:value') ==
      works_endpoint.filter(filter1='value1')
    )
    works_endpoint.filter.assert_called_with(filter1='value1')

class TestIsAlreadyDownload(object):
  def test_should_return_false_if_output_file_does_not_exist(self):
    m = download_works_utils
    with patch.object(m, 'load_meta_or_none') as load_meta_or_none_mock:
      with patch.object(m, 'FileSystems') as FileSystems:
        load_meta_or_none_mock.return_value = {
          'filter': FILTER_STR_1
        }
        FileSystems.exists.return_value = False
        assert not is_already_download(FILTER_STR_1, OUTPUT_FILE_1, OUTPUT_FILE_1 + '.meta')

  def test_should_return_false_if_meta_filter_does_not_match(self):
    m = download_works_utils
    with patch.object(m, 'load_meta_or_none') as load_meta_or_none_mock:
      with patch.object(m, 'FileSystems') as FileSystems:
        load_meta_or_none_mock.return_value = {
          'filter': 'other filter'
        }
        FileSystems.exists.return_value = True
        assert not is_already_download(FILTER_STR_1, OUTPUT_FILE_1, OUTPUT_FILE_1 + '.meta')

  def test_should_return_false_if_meta_file_does_not_exist(self):
    m = download_works_utils
    with patch.object(m, 'load_meta_or_none') as load_meta_or_none_mock:
      with patch.object(m, 'FileSystems') as FileSystems:
        load_meta_or_none_mock.return_value = None
        FileSystems.exists.return_value = True
        assert not is_already_download(FILTER_STR_1, OUTPUT_FILE_1, OUTPUT_FILE_1 + '.meta')

  def test_should_return_true_if_output_file_and_meta_file_matches(self):
    m = download_works_utils
    with patch.object(m, 'load_meta_or_none') as load_meta_or_none_mock:
      with patch.object(m, 'FileSystems') as FileSystems:
        load_meta_or_none_mock.return_value = {
          'filter': FILTER_STR_1
        }
        FileSystems.exists.return_value = True
        assert is_already_download(FILTER_STR_1, OUTPUT_FILE_1, OUTPUT_FILE_1 + '.meta')

class TestSaveItemsFromEndpointForFilterToZipfile(object):
  def test_should_pass_around_args(self):
    works_endpoint = MagicMock(spec=Works)
    m = download_works_utils
    with patch.object(m, 'get_works_endpoint_with_filter') as get_works_endpoint_with_filter_mock:
      with patch.object(m, 'save_items_to_zipfile') as save_items_to_zipfile_mock:
        with patch.object(m, 'is_already_download') as is_already_download_mock:
          with patch.object(m, 'FileSystems') as FileSystems:
            with patch.object(m, 'ZipFile') as ZipFile:
              is_already_download_mock.return_value = False
              save_items_to_zipfile_mock.return_value = 123
              get_works_endpoint_with_filter_mock.return_value.url = API_URL
              save_items_from_endpoint_for_filter_to_zipfile(
                works_endpoint, FILTER_STR_1, OUTPUT_FILE_1, ZIP_DEFLATED
              )
              get_works_endpoint_with_filter_mock.assert_called_with(
                works_endpoint, FILTER_STR_1
              )
              FileSystems.create.assert_any_call(OUTPUT_FILE_1)
              ZipFile.assert_called_with(
                FileSystems.create.return_value.__enter__(), 'w',
                compression=ZIP_DEFLATED, allowZip64=True
              )
              save_items_to_zipfile_mock.assert_called_with(
                get_works_endpoint_with_filter_mock.return_value,
                ZipFile.return_value.__enter__(),
                info=OUTPUT_FILE_1
              )

  def test_should_skip_if_file_already_downloaded(self):
    works_endpoint = MagicMock(spec=Works)
    m = download_works_utils
    with patch.object(m, 'get_works_endpoint_with_filter'):
      with patch.object(m, 'save_items_to_zipfile') as save_items_to_zipfile_mock:
        with patch.object(m, 'is_already_download') as is_already_download_mock:
          with patch.object(m, 'FileSystems'):
            with patch.object(m, 'ZipFile'):
              is_already_download_mock.return_value = True
              save_items_from_endpoint_for_filter_to_zipfile(
                works_endpoint, FILTER_STR_1, OUTPUT_FILE_1, ZIP_DEFLATED
              )
              save_items_to_zipfile_mock.assert_not_called()
