from datacapsule_crossref.doi_utils import (
  clean_doi,
  doi_to_normalised_key
)

SOME_DOI = '10.12345/path'

class TestCleanDoi(object):
  def test_should_return_none_if_doi_is_none(self):
    assert clean_doi(None) is None

  def test_should_strip_surround_spaces(self):
    assert clean_doi(' {} '.format(SOME_DOI)) == SOME_DOI

  def test_should_strip_surround_line_feeds(self):
    assert clean_doi('\n{}\n'.format(SOME_DOI)) == SOME_DOI

  def test_should_remove_line_feeds_within_doi(self):
    assert clean_doi('{}\n{}'.format(SOME_DOI[:2], SOME_DOI[2:])) == SOME_DOI

  def test_should_remove_tabs_within_doi(self):
    assert clean_doi('{}\t{}'.format(SOME_DOI[:2], SOME_DOI[2:])) == SOME_DOI

class TestNormaliseDoi(object):
  def test_should_return_none_if_doi_is_none(self):
    assert doi_to_normalised_key(None) is None

  def test_should_convert_to_lower_case(self):
    assert doi_to_normalised_key(SOME_DOI.upper()) == SOME_DOI.lower()

  def test_should_strip_surround_spaces(self):
    assert doi_to_normalised_key(' {} '.format(SOME_DOI)) == SOME_DOI

  def test_should_strip_surround_line_feeds(self):
    assert doi_to_normalised_key('\n{}\n'.format(SOME_DOI)) == SOME_DOI

  def test_should_remove_line_feeds_within_doi(self):
    assert doi_to_normalised_key('{}\n{}'.format(SOME_DOI[:2], SOME_DOI[2:])) == SOME_DOI

  def test_should_remove_tabs_within_doi(self):
    assert doi_to_normalised_key('{}\t{}'.format(SOME_DOI[:2], SOME_DOI[2:])) == SOME_DOI

  def test_should_remove_double_quotes_within_doi(self):
    assert doi_to_normalised_key('{}"{}'.format(SOME_DOI[:2], SOME_DOI[2:])) == SOME_DOI

  def test_should_replace_equals_with_forward_slash(self):
    assert doi_to_normalised_key(SOME_DOI.replace('/', '=')) == SOME_DOI
