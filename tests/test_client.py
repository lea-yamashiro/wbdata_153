import datetime as dt
import itertools
from unittest import mock

import pytest

from wbdata import client


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        pytest.param(
            [{"id": "USA", "name": "United States"}],
            "id    name\n----  -------------\nUSA   United States",
        ),
        pytest.param(
            [{"id": "WB", "value": "World Bank"}],
            "id    value\n----  ----------\nWB    World Bank",
        ),
    ],
)
def test_search_results_repr(data, expected):
    assert repr(client.SearchResult(data)) == expected


@pytest.mark.parametrize(
    ["arg", "expected"],
    [
        pytest.param("foo", "foo", id="string"),
        pytest.param(["foo", "bar", "baz"], "foo;bar;baz", id="list of strings"),
        pytest.param({1: "a", 2: "b", 3: "c"}, "1;2;3", id="dict of ints"),
        pytest.param(5.356, "5.356", id="float"),
    ],
)
def test_parse_value_or_iterable(arg, expected):
    assert client.parse_value_or_iterable(arg) == expected


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        pytest.param("5.1", 5.1, id="float"),
        pytest.param("3", 3.0, id="int"),
        pytest.param("heloooo", None, id="non-numeric"),
    ],
)
def test_cast_float(value, expected):
    assert client.cast_float(value) == expected


@pytest.fixture
def mock_client():
    with mock.patch("wbdata.client.fetcher.Fetcher", mock.Mock):
        yield client.Client()


@pytest.mark.parametrize(
    [
        "kwargs",
        "expected_url",
        "expected_args",
    ],
    [
        pytest.param(
            {"indicator": "FOO"},
            "https://api.worldbank.org/v2/countries/all/indicators/FOO",
            {},
            id="simple",
        ),
        pytest.param(
            {"indicator": "FOO", "country": "USA"},
            "https://api.worldbank.org/v2/countries/USA/indicators/FOO",
            {},
            id="one country",
        ),
        pytest.param(
            {"indicator": "FOO", "country": ["USA", "GBR"]},
            "https://api.worldbank.org/v2/countries/USA;GBR/indicators/FOO",
            {},
            id="two countries",
        ),
        pytest.param(
            {"indicator": "FOO", "date": "2005M02"},
            "https://api.worldbank.org/v2/countries/all/indicators/FOO",
            {"date": "2005"},
            id="date",
        ),
        pytest.param(
            {"indicator": "FOO", "date": ("2006M02", "2008M10"), "freq": "Q"},
            "https://api.worldbank.org/v2/countries/all/indicators/FOO",
            {"date": "2006Q1:2008Q4"},
            id="date and freq",
        ),
        pytest.param(
            {"indicator": "FOO", "source": "1"},
            "https://api.worldbank.org/v2/countries/all/indicators/FOO",
            {"source": "1"},
            id="one source",
        ),
        pytest.param(
            {"indicator": "FOO", "skip_cache": True},
            "https://api.worldbank.org/v2/countries/all/indicators/FOO",
            {},
            id="skip cache true",
        ),
    ],
)
def test_get_data_args(mock_client, kwargs, expected_url, expected_args):
    mock_client.fetcher.fetch = mock.Mock(return_value="Foo")
    mock_client.get_data(**kwargs)
    mock_client.fetcher.fetch.assert_called_once_with(
        expected_url, expected_args, skip_cache=kwargs.get("skip_cache", False)
    )


def test_parse_dates(mock_client):
    expected = [{"date": dt.datetime(2023, 4, 1)}]
    mock_client.fetcher.fetch = mock.Mock(return_value=[{"date": "2023Q2"}])
    got = mock_client.get_data("foo", parse_dates=True)
    assert got == expected


@pytest.mark.parametrize(
    ["url", "id_", "skip_cache", "expected_url"],
    [
        pytest.param("https://foo.bar", None, False, "https://foo.bar", id="no id"),
        pytest.param(
            "https://foo.bar", "baz", False, "https://foo.bar/baz", id="one id"
        ),
        pytest.param(
            "https://foo.bar",
            ["baz", "bat"],
            False,
            "https://foo.bar/baz;bat",
            id="two ids",
        ),
        pytest.param("https://foo.bar", None, True, "https://foo.bar", id="nocache"),
    ],
)
def test_id_only_query(mock_client, url, id_, skip_cache, expected_url):
    mock_client.fetcher.fetch = mock.Mock(return_value=[["foo"]])
    got = mock_client._id_only_query(url, id_, skip_cache=skip_cache)
    assert list(got) == ["foo"]
    mock_client.fetcher.fetch.assert_called_once_with(
        url=expected_url, skip_cache=skip_cache
    )


@pytest.mark.parametrize(
    ["function", "id_", "skip_cache", "expected_url"],
    [
        (function, id_, skip_cache, f"{host}{path}")
        for ((function, host), (id_, path), skip_cache) in itertools.product(
            (
                ("get_source", client.SOURCES_URL),
                ("get_incomelevel", client.ILEVEL_URL),
                ("get_topic", client.TOPIC_URL),
                ("get_lendingtype", client.LTYPE_URL),
            ),
            (
                (None, ""),
                ("foo", "/foo"),
                (["foo", "bar"], "/foo;bar"),
            ),
            (True, False),
        )
    ],
)
def test_id_only_functions(mock_client, function, id_, skip_cache, expected_url):
    mock_client.fetcher.fetch = mock.Mock(return_value=[["foo"]])
    got = getattr(mock_client, function)(id_, skip_cache=skip_cache)
    assert list(got) == ["foo"]
    mock_client.fetcher.fetch.assert_called_once_with(
        url=expected_url, skip_cache=skip_cache
    )


@pytest.mark.parametrize(
    ["country_id", "incomelevel", "lendingtype", "path", "args", "skip_cache"],
    (
        (cid, il, ltype, path, {**il_args, **ltype_args}, skip_cache)  # type: ignore[dict-item]
        for (
            (cid, path),
            (il, il_args),
            (ltype, ltype_args),
            skip_cache,
        ) in itertools.product(
            (
                (None, ""),
                ("foo", "/foo"),
                (["foo", "bar"], "/foo;bar"),
            ),
            (
                (None, {}),
                (2, {"incomeLevel": "2"}),
                ([2, 3], {"incomeLevel": "2;3"}),
            ),
            (
                (None, {}),
                (4, {"lendingType": "4"}),
                ([4, 5], {"lendingType": "4;5"}),
            ),
            (True, False),
        )
        if cid is None or (il is None and ltype is None)
    ),
)
def test_get_country(
    mock_client, country_id, incomelevel, lendingtype, path, args, skip_cache
):
    mock_client.fetcher.fetch = mock.Mock(return_value=[["foo"]])
    got = mock_client.get_country(country_id, incomelevel, lendingtype, skip_cache)
    assert list(got) == ["foo"]
    if country_id:
        mock_client.fetcher.fetch.assert_called_once_with(
            url=f"{client.COUNTRIES_URL}{path}", skip_cache=skip_cache
        )
    else:
        mock_client.fetcher.fetch.assert_called_once_with(
            url=f"{client.COUNTRIES_URL}{path}", args=args, skip_cache=skip_cache
        )
