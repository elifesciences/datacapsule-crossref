from __future__ import division

import logging

from six import iteritems
import pandas as pd

from datacapsule_crossref.csv_stats import (
  CsvStats,
  calculate_counts_from_df_batches,
  get_output_column_names,
  flatten_stats,
  Columns
)

def setup_module():
  logging.basicConfig(level='DEBUG')

class TestCsvStats(object):
  def test_should_merge_empty_stats_with_non_empty_stats(self):
    csv_stats_1 = CsvStats()
    csv_stats_2 = CsvStats()
    csv_stats_2.add_dataframe(pd.DataFrame({
      'a': [1, 2, 10, 11]
    }))
    csv_stats_1.add_stats(csv_stats_2)
    assert csv_stats_1.get_stats() == csv_stats_2.get_stats()

  def test_should_merge_non_empty_stats_with_empty_stats(self):
    csv_stats_1 = CsvStats()
    csv_stats_1.add_dataframe(pd.DataFrame({
      'a': [1, 2, 10, 11]
    }))
    non_merged_csv_stats = csv_stats_1.get_stats()
    csv_stats_2 = CsvStats()
    csv_stats_1.add_stats(csv_stats_2)
    assert csv_stats_1.get_stats() == non_merged_csv_stats

  def test_should_merge_non_empty_stats_with_non_empty_stats(self):
    csv_stats_1 = CsvStats()
    csv_stats_1.add_dataframe(pd.DataFrame({
      'a': [1, 2, 10, 11]
    }))
    non_merged_csv_stats = csv_stats_1.get_stats()
    csv_stats_2 = CsvStats()
    csv_stats_2.add_dataframe(pd.DataFrame({
      'a': [1, 2, 10, 11]
    }))
    csv_stats_1.add_stats(csv_stats_2)
    assert csv_stats_1.get_stats()['count'] == [8]

class TestCalculateCountsFromDfBatches(object):
  def test_empty_df_should_produce_zero_counts(self):
    result = calculate_counts_from_df_batches([
      pd.DataFrame([], columns=['a', 'b'])
    ])
    assert result == {
      'count': [0, 0],
      'count_valid': [0, 0]
    }

  def test_should_count_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, 10, 11]
    })])
    assert result['count'] == [4]

  def test_should_count_valid_int_columns_without_nan(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, None, 11]
    })])
    assert result['count_valid'] == [3]

  def test_should_count_numeric_int_columns_without_nan(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, None, 11]
    })])
    assert result['count_numeric'] == [3]

  def test_should_count_int_columns_including_nan(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, None, 11]
    })])
    assert result['count'] == [4]

  def test_should_count_bool_as_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': ['true', 'false', 'true', 'true']
    })])
    assert result['count'] == [4]
    assert result['count_non_zero'] == [3]
    assert result['count_zero'] == [1]

  def test_should_calculate_min_of_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, 10, 11]
    })])
    assert result['min'] == [1]

  def test_should_calculate_max_of_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, 10, 11]
    })])
    assert result['max'] == [11]

  def test_should_calculate_sum_of_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, 10, 11]
    })])
    assert result['sum'] == [24]

  def test_should_calculate_mean_of_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, 10, 11]
    })])
    assert result['mean'] == [24 / 4]

  def test_should_calculate_mean_including_zero_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, 0, 11]
    })])
    assert result['mean'] == [14 / 4]

  def test_should_calculate_mean_excluding_nan_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, pd.np.nan, 11]
    })])
    assert result['mean'] == [14 / 3]

  def test_should_calculate_mean_of_non_zero_int_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': [1, 2, 0, 11]
    })])
    assert result['mean_non_zero'] == [14 / 3]

  def test_should_count_str_columns(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': ['a1', 'a2', 'a3', 'a4']
    })])
    assert result['count'] == [4]

  def test_should_count_valid_str_columns_without_nan(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': ['a1', 'a2', pd.np.nan, 'a4']
    })])
    assert result['count_valid'] == [3]

  def test_should_count_str_columns_including_nan(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': ['a1', 'a2', pd.np.nan, 'a4']
    })])
    assert result['count'] == [4]

  def test_should_count_str_columns_containing_some_numbers(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': ['a1', 'a2', '3', 'a4']
    })])
    assert result['count'] == [4]
    assert result['count_valid'] == [4]
    assert 'count_numeric' not in result

  def test_should_count_str_columns_containing_some_numbers_across_batches(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': ['a1', 'a2']
    }), pd.DataFrame({
      'a': ['3', '4']
    })])
    assert result['count'] == [4]
    assert result['count_valid'] == [4]
    assert 'count_numeric' not in result

  def test_should_count_str_columns_containing_some_numbers_in_first_batch(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': ['1', '2']
    }), pd.DataFrame({
      'a': ['a3', 'a4']
    })])
    assert result['count'] == [4]
    assert result['count_valid'] == [4]
    assert 'count_numeric' not in result

  def test_should_count_str_columns_containing_some_bool(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'a': ['a1', 'a2', 'false', 'a4']
    })])
    assert result['count'] == [4]
    assert 'count_non_zero' not in result

  def test_should_count_int_columns_grouped_by_column(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'g': ['x', 'y', 'x', 'x'],
      'a': [1, 2, 10, 11]
    })], groupby_columns='g')
    assert result['x']['count'] == [3]
    assert result['y']['count'] == [1]

  def test_should_count_int_columns_grouped_by_multiple_column(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'g1': ['g1_x', 'g1_y', 'g1_x', 'g1_x'],
      'g2': ['g2_x', 'g2_y', 'g2_y', 'g2_y'],
      'a': [1, 2, 10, 11]
    })], groupby_columns=['g1', 'g2'])
    assert {g: v['count'] for g, v in iteritems(result)} == {
      ('g1_x', 'g2_x'): [1],
      ('g1_x', 'g2_y'): [2],
      ('g1_y', 'g2_y'): [1]
    }

  def test_should_count_int_columns_grouped_by_multiple_column_with_nan(self):
    result = calculate_counts_from_df_batches([pd.DataFrame({
      'g1': ['g1_x', 'g1_y', 'g1_x', 'g1_x'],
      'g2': ['g2_x', None, 'g2_y', 'g2_y'],
      'a': [1, 2, 10, 11]
    })], groupby_columns=['g1', 'g2'])
    assert {g: v['count'] for g, v in iteritems(result)} == {
      ('g1_x', 'g2_x'): [1],
      ('g1_x', 'g2_y'): [2],
      ('g1_y', ''): [1]
    }

class TestGetOutputColumnNames(object):
  def test_should_return_columns_without_groupby_columns(self):
    assert get_output_column_names(['a', 'b'], None) == [Columns.STAT, 'a', 'b']

  def test_should_return_columns_with_single_groupby_columns(self):
    assert (
      get_output_column_names(['a', 'b'], ['g1']) ==
      ['g1', Columns.STAT, 'a', 'b']
    )

  def test_should_return_columns_with_multiple_groupby_columns(self):
    assert (
      get_output_column_names(['a', 'b'], ['g1', 'g2']) ==
      ['g1', 'g2', Columns.STAT, 'a', 'b']
    )

class TestFlattenStats(object):
  def test_should_flatten_stats_without_groupby_columns(self):
    assert list(flatten_stats({
      'count': [1]
    }, ['a'], None)) == [{
      Columns.STAT: 'count',
      'a': 1
    }]

  def test_should_flatten_stats_with_single_groupby_columns(self):
    assert list(flatten_stats({
      'g1_x': {
        'count': [1]
      }
    }, ['a'], ['g1'])) == [{
      'g1': 'g1_x',
      Columns.STAT: 'count',
      'a': 1
    }]

  def test_should_flatten_stats_with_multiple_groupby_columns(self):
    assert list(flatten_stats({
      ('g1_x', 'g2_x'): {
        'count': [1]
      }
    }, ['a'], ['g1', 'g2'])) == [{
      'g1': 'g1_x',
      'g2': 'g2_x',
      Columns.STAT: 'count',
      'a': 1
    }]
