from datacapsule_crossref.utils.collection import (
  extend_dict
)

from datacapsule_crossref.extract_summaries_from_works import (
  Columns as SummaryColumns
)

from datacapsule_crossref.reference_stats import (
  TypedCounterWithExamples
)

from datacapsule_crossref.extract_utils import (
  update_reference_counters_for_summary
)

DOI_1 = 'doi_1'
PUBLISHER_1 = 'publisher 1'
CONTAINER_TITLE_1 = 'container title 1'
FIRST_SUBJECT_AREA_1 = 'first subject area 1'
CREATED_YEAR_1 = 2000
CREATED_1 = '%d-01-01T11:22:33Z' % CREATED_YEAR_1

WORK_SUMMARY_1 = {
  SummaryColumns.DOI: DOI_1,
  SummaryColumns.PUBLISHER: PUBLISHER_1,
  SummaryColumns.CONTAINER_TITLE: CONTAINER_TITLE_1,
  SummaryColumns.FIRST_SUBJECT_AREA: FIRST_SUBJECT_AREA_1,
  SummaryColumns.CREATED: CREATED_1,
  SummaryColumns.REFERENCE_COUNT: 0,
  SummaryColumns.HAS_REFERENCES: 0,
  SummaryColumns.DEBUG: ''
}

TOTAL_COUNTERS = {
  'total_container_title', 'total_first_subject_area', 'total_publisher', 'total_created'
}

NONOA_COUNTERS = {
  'non_oa_ref_container_title', 'non_oa_ref_first_subject_area',
  'non_oa_ref_publisher', 'non_oa_ref_created'
}

class TestUpdateReferenceCountersForSummary(object):
  def test_should_extract_totals(self):
    result = update_reference_counters_for_summary(TypedCounterWithExamples(10), WORK_SUMMARY_1)
    assert {counter_type for counter_type, _, _, _ in result} == TOTAL_COUNTERS

  def test_should_add_doi_as_example(self):
    result = update_reference_counters_for_summary(TypedCounterWithExamples(10), WORK_SUMMARY_1)
    assert {examples[0] for _, _, _, examples in result} == {DOI_1}

  def test_should_add_non_oa_if_reference_count_is_non_zera_but_no_references(self):
    result = update_reference_counters_for_summary(TypedCounterWithExamples(10), extend_dict(
      WORK_SUMMARY_1, {
        SummaryColumns.REFERENCE_COUNT: 1,
        SummaryColumns.HAS_REFERENCES: 0
      }
    ))
    assert {counter_type for counter_type, _, _, _ in result} == TOTAL_COUNTERS | NONOA_COUNTERS

  def test_should_not_add_non_oa_if_references_are_available(self):
    result = update_reference_counters_for_summary(TypedCounterWithExamples(10), extend_dict(
      WORK_SUMMARY_1, {
        SummaryColumns.REFERENCE_COUNT: 1,
        SummaryColumns.HAS_REFERENCES: 1
      }
    ))
    assert {counter_type for counter_type, _, _, _ in result} == TOTAL_COUNTERS
