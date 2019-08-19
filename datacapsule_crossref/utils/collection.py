from collections import deque
from heapq import heappush, heappop, heappushpop
from functools import partial
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
        key = _identity
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


def _wrap_with_index(unwrapped, key_fn, indices):
    return key_fn(unwrapped), next(indices), unwrapped


def _unwrap_with_index(wrapped):
    return wrapped[2]


def _identity(x):
    return x


def iter_sort_window(
        iterable, window_size, key=None, remove_duplicates=False, on_dropped_item=None):

    if window_size <= 1:
        for item in iterable:
            yield item
        return
    if key:
        # we can't have equal keys, as the unwrapped may not be ordered
        # add indices as the second value in case the provided key is the same
        indices = itertools.count()
        wrap = partial(_wrap_with_index, key_fn=key, indices=indices)
        unwrap = _unwrap_with_index
    else:
        wrap = _identity
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
                    'failed to pop item from heap, length: {} (key f={})'.format(
                        len(heap), heap[:100])
                ), e)
    if remove_duplicates:
        uniq_items = iter_uniq_window(
            iter_sort(), window_size, key=key, on_dropped_item=on_dropped_item
        )
        for item in uniq_items:
            yield item
    else:
        for item in iter_sort():
            yield item


def iter_batch(coll, batch_size):
    batch = []
    for x in coll:
        batch.append(x)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def peek(iterable):
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return first, itertools.chain([first], iterable)


def iter_dict_to_list(iterable, fields):
    return (
        [item.get(field) for field in fields]
        for item in iterable
    )


def extend_dict(d, *other_dicts, **kwargs):
    """
    example:
    extend_dict(d1, d2)
    is equivalent to Python 3 syntax:
    {
      **d1,
      **d2
    }
    """
    d = d.copy()
    for other_dict in other_dicts:
        d.update(other_dict)
    d.update(kwargs)
    return d
