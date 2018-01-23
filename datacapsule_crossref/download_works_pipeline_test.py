from mock import patch, MagicMock

from crossref.restful import Works

from datacapsule_crossref.utils.collection import (
  to_namedtuple
)

import datacapsule_crossref.download_works_pipeline as download_works_pipeline
from datacapsule_crossref.download_works_pipeline import (
  get_target_filter_map,
  get_works_endpoint,
  APPLICATION_NAME,
  APPLICATION_VERSION,
  APPLICATION_URL
)

FILTER_KEY_1 = 'key1'
FILTER_VALUE_1 = 'value1'
FILTER_1 = '%s:%s' % (FILTER_KEY_1, FILTER_VALUE_1)
FILTER_NAME_1 = 'filter1'
EMAIL = 'email1@dev.null'

class TestTargetFilterMap(object):
  def test_should_create_single_map(self):
    works_endpoint = MagicMock(spec=Works)
    opt = to_namedtuple(filter=FILTER_1, filter_name=FILTER_NAME_1, group_by_published_date=False)
    assert get_target_filter_map(works_endpoint, opt) == {
      FILTER_NAME_1: FILTER_1
    }

  def test_should_return_filters_based_on_year_count(self):
    works_endpoint = MagicMock(spec=Works)
    opt = to_namedtuple(filter=None, filter_name=None, group_by_published_date=True)
    m = download_works_pipeline
    with patch.object(m, 'group_year_counts_to_filters_by_target') as\
      group_year_counts_to_filters_by_target:
      with patch.object(m, 'get_published_year_counts') as get_published_year_counts:
        assert (
          get_target_filter_map(works_endpoint, opt) ==
          group_year_counts_to_filters_by_target.return_value
        )
        group_year_counts_to_filters_by_target.assert_called_with(
          get_published_year_counts.return_value
        )
        get_published_year_counts.assert_called_with(
          works_endpoint
        )

  def test_should_add_filter_when_grouping_based_on_year_count(self):
    works_endpoint = MagicMock(spec=Works)
    opt = to_namedtuple(filter=FILTER_1, filter_name=None, group_by_published_date=True)
    m = download_works_pipeline
    with patch.object(m, 'group_year_counts_to_filters_by_target') as\
      group_year_counts_to_filters_by_target:
      with patch.object(m, 'get_published_year_counts') as get_published_year_counts:
        assert (
          get_target_filter_map(works_endpoint, opt) ==
          group_year_counts_to_filters_by_target.return_value
        )
        group_year_counts_to_filters_by_target.assert_called_with(
          get_published_year_counts.return_value
        )
        get_published_year_counts.assert_called_with(
          works_endpoint.filter.return_value
        )
        works_endpoint.filter.assert_called_with(**{
          FILTER_KEY_1: FILTER_VALUE_1
        })

class TestGetWorksEndpoint(object):
  def test_should_instantiate_works_without_etiquette(self):
    opt = to_namedtuple(email=None)
    m = download_works_pipeline
    with patch.object(m, 'Works') as WorksMock:
      assert get_works_endpoint(opt) == WorksMock.return_value
      WorksMock.assert_called_with()

  def test_should_instantiate_works_with_etiquette(self):
    opt = to_namedtuple(email=EMAIL)
    m = download_works_pipeline
    with patch.object(m, 'Etiquette') as EtiquetteMock:
      with patch.object(m, 'Works') as WorksMock:
        assert get_works_endpoint(opt) == WorksMock.return_value
        WorksMock.assert_called_with(etiquette=EtiquetteMock.return_value)
        EtiquetteMock.assert_called_with(
          APPLICATION_NAME, APPLICATION_VERSION, APPLICATION_URL, opt.email
        )
