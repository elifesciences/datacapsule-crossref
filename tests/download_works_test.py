import logging
import json
from unittest.mock import MagicMock
from asyncio import Future
from io import BytesIO
from functools import wraps
from urllib3.exceptions import ProtocolError

import pytest

from datacapsule_crossref.download_works import (
    add_url_parameters,
    PageResponseIterator
)


LOGGER = logging.getLogger(__name__)

TEST_CROSSREF_API_URL = 'test://crossref/api/works'

CURSOR_1 = 'cursor1'
CURSOR_2 = 'cursor2'


@pytest.fixture(name='session_mock')
def _session_mock():
    mock = MagicMock(name='session')
    return mock


def _resolved_future(result) -> Future:
    future = Future()
    future.set_result(result)
    return future


def _mock_session_response(data: bytes):
    data_stream = BytesIO(data)
    mock = MagicMock(name='session_response')
    mock.raw.read = data_stream.read
    return mock


def _sequential_side_effects(side_effects: list):
    iterator = iter(side_effects)

    def side_effect_wrapper(*args, **kwargs):
        side_effect = next(iterator)
        if isinstance(side_effect, Exception):
            raise side_effect
        if isinstance(side_effect, MagicMock):
            return side_effect
        if callable(side_effect):
            return side_effect(*args, **kwargs)
        return side_effect
    return side_effect_wrapper


def _mock_incomplete_read_session_response(data: bytes):
    data_stream = BytesIO(data)
    mock = MagicMock(name='session_response')
    mock.raw.read.side_effect = _sequential_side_effects([
        data_stream.read,
        ProtocolError('IncompleteRead')
    ])
    return mock


def _mock_session_get(url_to_response_map: dict):
    @wraps(url_to_response_map.__getitem__)
    def wrapper(url, **kwargs):
        response = url_to_response_map[url]
        if callable(response):
            response = response(url, **kwargs)
            LOGGER.debug('callable response: %s', response)
        if isinstance(response, bytes):
            return _resolved_future(_mock_session_response(response))
        if not isinstance(response, Future):
            return _resolved_future(response)
        return response
    return wrapper


def _mock_response_data(status: str = 'ok', next_cursor: str = None):
    return json.dumps({
        'status': status,
        'message': {
            'next-cursor': next_cursor
        }
    }).encode('utf-8')


def _url_with_cursor(base_url: str, cursor: str):
    return '%s?cursor=%s' % (base_url, cursor)


def _page_responses_from_session(*args, **kwargs):
    iterator = PageResponseIterator(*args, **kwargs)
    result = list(iterator)
    LOGGER.debug('result: %s', result)
    assert not iterator.get_cache_size()
    return result


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


class TestPageResponseIterator:
    def test_should_request_single_page(
            self, session_mock: MagicMock):
        response_data = _mock_response_data()
        session_mock.get = _mock_session_get({
            _url_with_cursor(TEST_CROSSREF_API_URL, CURSOR_1): response_data
        })
        assert _page_responses_from_session(
            session=session_mock,
            base_url=TEST_CROSSREF_API_URL,
            start_cursor=CURSOR_1
        ) == [
            (None, response_data)
        ]

    def test_should_request_multiple_pages(
            self, session_mock: MagicMock):
        response_data_1 = _mock_response_data(next_cursor=CURSOR_2)
        response_data_2 = _mock_response_data()
        session_mock.get = _mock_session_get({
            _url_with_cursor(TEST_CROSSREF_API_URL, CURSOR_1): response_data_1,
            _url_with_cursor(TEST_CROSSREF_API_URL, CURSOR_2): response_data_2
        })
        assert _page_responses_from_session(
            session=session_mock,
            base_url=TEST_CROSSREF_API_URL,
            start_cursor=CURSOR_1
        ) == [
            (CURSOR_2, response_data_1),
            (None, response_data_2)
        ]

    def test_should_retry_on_incomplete_read(
            self, session_mock: MagicMock):
        response_data_1 = _mock_response_data(next_cursor=CURSOR_2)
        response_1_incomplete = _mock_incomplete_read_session_response(response_data_1)
        response_data_2 = _mock_response_data()
        session_mock.get = _mock_session_get({
            _url_with_cursor(TEST_CROSSREF_API_URL, CURSOR_1): _sequential_side_effects([
                response_1_incomplete,
                response_data_1
            ]),
            _url_with_cursor(TEST_CROSSREF_API_URL, CURSOR_2): response_data_2
        })
        assert _page_responses_from_session(
            session=session_mock,
            base_url=TEST_CROSSREF_API_URL,
            start_cursor=CURSOR_1
        ) == [
            (CURSOR_2, response_data_1),
            (None, response_data_2)
        ]
