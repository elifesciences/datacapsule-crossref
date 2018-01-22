import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

def get_logger():
  return logging.getLogger(__name__)

def MapOrLog(fn, log_fn=None, error_count=None):
  if log_fn is None:
    log_fn = lambda e, x: (
      get_logger().warning(
        'caught exception (ignoring item): %s, input: %.100s...',
        e, x, exc_info=e
      )
    )
  error_counter = (
    Metrics.counter('MapOrLog', error_count)
    if error_count
    else None
  )
  def wrapper(x):
    try:
      yield fn(x)
    except Exception as e:
      if error_counter:
        error_counter.inc()
      log_fn(e, x)
  return beam.FlatMap(wrapper)
