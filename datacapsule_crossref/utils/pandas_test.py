import csv
from io import StringIO

from .pandas import read_csv_with_default_dtype


def _to_csv(rows):
  buffer = StringIO()
  writer = csv.writer(buffer)
  writer.writerows(rows)
  return buffer.getvalue()


class TestReadCsvWithDefaultDtype(object):
  def test_should_return_batches_without_column_types(self):
    df_batches = list(read_csv_with_default_dtype(
      StringIO(_to_csv([
        ['column1', 'column2'],
        ['row1.1', 'row1.2'],
        ['row2.1', 'row2.2'],
        ['row3.1', 'row3.2']
      ])),
      chunksize=2
    ))
    assert len(df_batches) == 2
    assert list(df_batches[0]['column1']) == ['row1.1', 'row2.1']
    assert list(df_batches[0]['column2']) == ['row1.2', 'row2.2']
    assert list(df_batches[1]['column1']) == ['row3.1']
    assert list(df_batches[1]['column2']) == ['row3.2']

  def test_should_return_batches_with_column_types(self):
    df_batches = list(read_csv_with_default_dtype(
      StringIO(_to_csv([
        ['column1', 'column2'],
        ['11', '12'],
        ['21', '22'],
        ['31', '32']
      ])),
      chunksize=2,
      dtype={
        'column1': int
      },
      default_dtype='object'
    ))
    assert len(df_batches) == 2
    assert df_batches[0]['column1'].dtype == int
    assert df_batches[0]['column2'].dtype == object
    assert list(df_batches[0]['column1']) == [11, 21]
    assert list(df_batches[0]['column2']) == ['12', '22']
    assert list(df_batches[1]['column1']) == [31]
    assert list(df_batches[1]['column2']) == ['32']
