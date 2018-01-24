from apache_beam import CombineFn

from datacapsule_crossref.reference_stats import (
  TypedCounterWithExamples
)

from datacapsule_crossref.extract_utils import (
  update_reference_counters_for_summary
)

class ReferenceStatsCombineFn(CombineFn):
  def create_accumulator(self, *args, **kwargs):
    return TypedCounterWithExamples(10)

  def add_input(self, accumulator, element, *args, **kwargs):
    """Return result of folding element into accumulator.

    CombineFn implementors must override add_input.

    Args:
      accumulator: the current accumulator
      element: the element to add
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    update_reference_counters_for_summary(accumulator, element)
    return accumulator

  def add_inputs(self, accumulator, elements, *args, **kwargs):
    """Returns the result of folding each element in elements into accumulator.

    This is provided in case the implementation affords more efficient
    bulk addition of elements. The default implementation simply loops
    over the inputs invoking add_input for each one.

    Args:
      accumulator: the current accumulator
      elements: the elements to add
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    for element in elements:
      accumulator = self.add_input(accumulator, element, *args, **kwargs)
    return accumulator

  def merge_accumulators(self, accumulators, *args, **kwargs):
    """Returns the result of merging several accumulators
    to a single accumulator value.

    Args:
      accumulators: the accumulators to merge
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    result = self.create_accumulator()
    for accumulator in accumulators:
      result.add_counter(accumulator)
    return result

  def extract_output(self, accumulator, *args, **kwargs):
    """Return result of converting accumulator into the output value.

    Args:
      accumulator: the final accumulator value computed by this CombineFn
        for the entire input key or PCollection.
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    return list(accumulator)
