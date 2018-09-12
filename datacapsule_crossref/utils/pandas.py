import logging
from io import StringIO

import pandas as pd

from .collection import extend_dict

LOGGER = logging.getLogger(__name__)


def read_csv_with_default_dtype(
  f, *args, dtype=None, default_dtype=None, chunksize=None, **kwargs):

  if not default_dtype:
    return pd.read_csv(f, *args, dtype=dtype, chunksize=chunksize, **kwargs)

  LOGGER.debug('kwargs: %s', kwargs)

  header_row = f.readline()
  columns = list(pd.read_csv(
    StringIO(header_row),
    *args,
    **kwargs
  ).columns)

  LOGGER.info('columns: %s', columns)

  dtype = extend_dict(
    dict(zip(columns, ['object'] * len(columns))),
    dtype
  )

  return pd.read_csv(
    f, *args, **kwargs,
    header=None, names=columns, dtype=dtype, chunksize=chunksize
  )
