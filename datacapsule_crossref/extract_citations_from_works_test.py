from .extract_citations_from_works import (
  extract_doi_from_reference
)


DOI_1 = 'doi1'


class TestExtractDoiFromReference(object):
  def test_should_extract_doi(self):
    assert extract_doi_from_reference({
      'DOI': DOI_1
    }) == DOI_1
