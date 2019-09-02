from datacapsule_crossref.extract_summaries_from_works import clean_text


class TestCleanText:
    def test_should_replace_line_feed_with_space(self):
        assert clean_text('a\nb') == 'a b'

    def test_should_replace_carriage_return_with_space(self):
        assert clean_text('a\rb') == 'a b'

    def test_should_replace_tab_with_space(self):
        assert clean_text('a\tb') == 'a b'

    def test_should_replace_double_space_with_single_space(self):
        assert clean_text('a  b') == 'a b'

    def test_should_replace_multiple_whitespace_with_single_space(self):
        assert clean_text('a \r\n b') == 'a b'
