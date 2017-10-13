from collections import deque
from heapq import heappush, heappop, heappushpop
import itertools

from future.utils import raise_from

class SimpleCounter(object):
  def __init__(self):
    self.data = {}

  def up(self, key):
    self.data[key] = self.get(key) + 1

  def down(self, key):
    updated_value = self.get(key) - 1
    if updated_value == 0:
      del self.data[key]
    if updated_value < 0:
      raise IndexError('value already zero: {}'.format(key))

  def get(self, key):
    return self.data.get(key, 0)

def iter_uniq_window(iterable, window_size, key=None, on_dropped_item=None):
  if key is None:
    key = lambda v: v
  previous_keys = deque()
  counts = SimpleCounter()
  for item in iterable:
    k = key(item)
    if not counts.get(k):
      yield item
      if len(previous_keys) == window_size:
        counts.down(previous_keys.pop())
      previous_keys.append(k)
      counts.up(k)
    elif on_dropped_item:
      on_dropped_item(item)

def iter_sort_window(
  iterable, window_size, key=None, remove_duplicates=False, on_dropped_item=None):

  if window_size <= 1:
    yield from iterable
    return
  if key:
    # we can't have equal keys, as the unwrapped may not be ordered
    # add indices as the second value in case the provided key is the same
    indices = itertools.count()
    wrap = lambda unwrapped: (key(unwrapped), next(indices), unwrapped)
    unwrap = lambda wrapped: wrapped[2]
  else:
    wrap = lambda x: x
    unwrap = wrap
  def iter_sort():
    heap = []
    window_size_minus_1 = window_size - 1
    for item in iterable:
      if len(heap) == window_size_minus_1:
        yield unwrap(heappushpop(heap, wrap(item)))
      else:
        heappush(heap, wrap(item))
    while heap:
      try:
        yield unwrap(heappop(heap))
      except TypeError as e:
        raise_from(TypeError(
          'failed to pop item from heap, length: {} (key f={})'.format(len(heap), heap[:100])
        ), e)
  if remove_duplicates:
    yield from iter_uniq_window(
      iter_sort(), window_size, key=key, on_dropped_item=on_dropped_item
    )
  else:
    yield from iter_sort()

def iter_batch(coll, batch_size):
  batch = []
  for x in coll:
    batch.append(x)
    if len(batch) >= batch_size:
      yield batch
      batch = []
  if batch:
    yield batch
