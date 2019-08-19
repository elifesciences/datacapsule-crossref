from datacapsule_crossref.download_works import (
    add_url_parameters
)


class TestAddUrlParameters(object):
    def test_should_add_quotation_mark(self):
        assert (
            add_url_parameters('http://somewhere', 'value=123') ==
            'http://somewhere?value=123'
        )

    def test_should_add_ampersand_mark(self):
        assert (
            add_url_parameters('http://somewhere?other=abc', 'value=123') ==
            'http://somewhere?other=abc&value=123'
        )

    def test_should_format_dict(self):
        assert (
            add_url_parameters('http://somewhere', {'value': '123'}) ==
            'http://somewhere?value=123'
        )

    def test_should_format_list(self):
        assert (
            add_url_parameters('http://somewhere', [('value', '123')]) ==
            'http://somewhere?value=123'
        )

    def test_should_urlencode_special_characters_using_dict(self):
        assert (
            add_url_parameters('http://somewhere', {'value': '?& '}) ==
            r'http://somewhere?value=%3F%26+'
        )

    def test_should_urlencode_special_characters_using_list(self):
        assert (
            add_url_parameters('http://somewhere', [('value', '?& ')]) ==
            r'http://somewhere?value=%3F%26+'
        )
