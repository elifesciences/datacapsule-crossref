from datacapsule_crossref.reference_stats import (
  CounterWithExamples,
  TypedCounterWithExamples
)

COUNTER_TYPE_1 = 'counter1'
COUNTER_TYPE_2 = 'counter2'

KEY_1 = 'key1'
KEY_2 = 'key2'

EXAMPLE_1 = 'example1'
EXAMPLE_2 = 'example2'
EXAMPLE_3 = 'example3'

class TestCounterWithExamples(object):
  def test_should_count_one(self):
    counter = CounterWithExamples(10)
    counter.add(KEY_1, EXAMPLE_1)
    assert list(counter) == [(KEY_1, 1, [EXAMPLE_1])]

  def test_should_count_two(self):
    counter = CounterWithExamples(10)
    counter.add(KEY_1, EXAMPLE_1)
    counter.add(KEY_1, EXAMPLE_2)
    assert list(counter) == [(KEY_1, 2, [EXAMPLE_1, EXAMPLE_2])]

  def test_should_limit_examples(self):
    counter = CounterWithExamples(1)
    counter.add(KEY_1, EXAMPLE_1)
    counter.add(KEY_1, EXAMPLE_2)
    assert list(counter) == [(KEY_1, 2, [EXAMPLE_1])]

  def test_should_merge_count_into_empty_counter(self):
    counter1 = CounterWithExamples(10)
    counter1.add(KEY_1, EXAMPLE_1)
    counter2 = CounterWithExamples(10)
    counter2.add_counter(counter1)
    assert list(counter2) == [(KEY_1, 1, [EXAMPLE_1])]

  def test_should_merge_count_into_existing_key(self):
    counter1 = CounterWithExamples(10)
    counter1.add(KEY_1, EXAMPLE_1)
    counter2 = CounterWithExamples(10)
    counter2.add(KEY_1, EXAMPLE_2)
    counter2.add_counter(counter1)
    assert list(counter2) == [(KEY_1, 2, [EXAMPLE_2, EXAMPLE_1])]

  def test_should_limit_examples_when_merging(self):
    counter1 = CounterWithExamples(10)
    counter1.add(KEY_1, EXAMPLE_1)
    counter1.add(KEY_1, EXAMPLE_2)
    counter2 = CounterWithExamples(2)
    counter2.add(KEY_1, EXAMPLE_3)
    counter2.add_counter(counter1)
    assert list(counter2) == [(KEY_1, 3, [EXAMPLE_3, EXAMPLE_1])]

class TestTypedCounterWithExample(object):
  def test_should_count_one(self):
    typed_counter = TypedCounterWithExamples(10)
    typed_counter.add(COUNTER_TYPE_1, KEY_1, EXAMPLE_1)
    assert list(typed_counter) == [(COUNTER_TYPE_1, KEY_1, 1, [EXAMPLE_1])]

  def test_should_merge_count_into_empty_counter(self):
    typed_counter1 = TypedCounterWithExamples(10)
    typed_counter1.add(COUNTER_TYPE_1, KEY_1, EXAMPLE_1)
    typed_counter2 = TypedCounterWithExamples(10)
    typed_counter2.add_counter(typed_counter1)
    assert list(typed_counter2) == [(COUNTER_TYPE_1, KEY_1, 1, [EXAMPLE_1])]

  def test_should_merge_count_into_existing_key(self):
    typed_counter1 = TypedCounterWithExamples(10)
    typed_counter1.add(COUNTER_TYPE_1, KEY_1, EXAMPLE_1)
    typed_counter2 = TypedCounterWithExamples(10)
    typed_counter2.add(COUNTER_TYPE_1, KEY_1, EXAMPLE_2)
    typed_counter2.add_counter(typed_counter1)
    assert list(typed_counter2) == [(COUNTER_TYPE_1, KEY_1, 2, [EXAMPLE_2, EXAMPLE_1])]
