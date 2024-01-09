import json
from unittest import mock

import pytest

from wbdata import fetcher


@pytest.fixture
def mocked_fetcher() -> fetcher.Fetcher:
    return fetcher.Fetcher(cache={}, session=mock.Mock())


class MockHTTPResponse:
    def __init__(self, value):
        self.text = json.dumps(value)


def test_get_request_content(mocked_fetcher):
    url = "http://foo.bar"
    params = {"baz": "bat"}
    expected = {"hello": "there"}
    mocked_fetcher.session.get = mock.Mock(
        return_value=MockHTTPResponse(value=expected)
    )
    result = mocked_fetcher._get_response_body(url=url, params=params)
    mocked_fetcher.session.get.assert_called_once_with(url=url, params=params)
    assert json.loads(result) == expected


@pytest.mark.parametrize(
    ["url", "params", "response", "expected"],
    (
        pytest.param(
            "http://foo.bar",
            {"baz": "bat"},
            [{"page": "1", "pages": "1"}, [{"hello": "there"}]],
            fetcher.ParsedResponse(
                rows=[{"hello": "there"}],
                page=1,
                pages=1,
                last_updated=None,
            ),
            id="No date",
        ),
        pytest.param(
            "http://foo.bar",
            {"baz": "bat"},
            [
                {"page": "1", "pages": "1", "lastupdated": "01/02/2023"},
                [{"hello": "there"}],
            ],
            fetcher.ParsedResponse(
                rows=[{"hello": "there"}],
                page=1,
                pages=1,
                last_updated="01/02/2023",
            ),
            id="with date",
        ),
    ),
)
def test_get_response(url, params, response, expected, mocked_fetcher):
    mocked_fetcher.session.get = mock.Mock(
        return_value=MockHTTPResponse(value=response)
    )
    got = mocked_fetcher._get_response(url=url, params=params)
    mocked_fetcher.session.get.assert_called_once_with(url=url, params=params)
    assert got == expected
    assert mocked_fetcher.cache[(url), (("baz", "bat"),)] == json.dumps(response)
