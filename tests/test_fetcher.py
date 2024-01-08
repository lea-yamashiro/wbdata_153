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
    assert mocked_fetcher.session.get.called_once_with(url=url, params=params)
    assert json.loads(result) == expected
