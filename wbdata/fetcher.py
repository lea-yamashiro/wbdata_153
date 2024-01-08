"""
wbdata.fetcher: retrieve and cache queries
"""

import contextlib
import dataclasses
import datetime as dt
import json
import logging
import pprint
from typing import Any, Dict, List, MutableMapping, NamedTuple, Tuple, Union

import backoff
import requests

from .types import Row

PER_PAGE = 1000
TRIES = 3


def _strip_id(row: Row) -> None:
    with contextlib.suppress(KeyError):
        row["id"] = row["id"].strip()  # type: ignore[union-attr]


Response = Tuple[Dict[str, Any], List[Dict[str, Any]]]


class ParsedResponse(NamedTuple):
    rows: List[Row]
    page: int
    pages: int
    last_updated: Union[str, None]


def _parse_response(response: Response) -> ParsedResponse:
    try:
        return ParsedResponse(
            rows=response[1],
            page=int(response[0]["page"]),
            pages=int(response[0]["pages"]),
            last_updated=response[0].get("lastupdated"),
        )
    except (IndexError, KeyError) as e:
        try:
            message = response[0]["message"][0]
            raise RuntimeError(
                f"Got error {message['id']} ({message['key']}): " f"{message['value']}"
            ) from e
        except (IndexError, KeyError) as e:
            raise RuntimeError(
                f"Got unexpected response:\n{pprint.pformat(response)}"
            ) from e


CacheKey = Tuple[str, Tuple[Tuple[str, Any], ...]]


@dataclasses.dataclass
class Fetcher:
    cache: MutableMapping[CacheKey, str]
    session: requests.Session = dataclasses.field(default_factory=requests.Session)

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=requests.exceptions.ConnectTimeout,
        max_tries=TRIES,
    )
    def _get_response_body(
        self,
        url: str,
        params: Dict[str, Any],
    ) -> str:
        """
        Fetch a url directly from the World Bank

        :url: the url to retrieve
        :params: a dictionary of GET parameters
        :returns: a string with the response content
        """
        return self.session.get(url=url, params=params).text

    def _get_response(
        self,
        url: str,
        params: Dict[str, Any],
        skip_cache=False,
    ) -> ParsedResponse:
        """
        Get single page response from World Bank API or from cache
        : query_url: the base url to be queried
        : params: a dictionary of GET arguments
        : skip_cache: bypass the cache
        : returns: a dictionary with the response from the API
        """
        key = (url, tuple(sorted(params.items())))
        if not skip_cache and key in self.cache:
            body = self.cache[key]
        else:
            body = self._get_response_body(url, params)
            self.cache[key] = body
        return _parse_response(tuple(json.loads(body)))

    def fetch(
        self,
        url: str,
        params=None,
        skip_cache=False,
    ) -> Tuple[List[Row], Union[dt.datetime, None]]:
        """Fetch data from the World Bank API or from cache.

        Given the base url, keep fetching results until there are no more pages.

        : query_url: the base url to be queried
        : params: a dictionary of GET arguments
        : skip_cache: use the cache
        : returns: a list of dictionaries containing the response to the query
        """
        params = params or {}
        params["format"] = "json"
        params["per_page"] = PER_PAGE
        page, pages = -1, -2
        rows: List[Row] = []
        while pages != page:
            response = self._get_response(
                url=url,
                params=params,
                skip_cache=skip_cache,
            )
            rows.extend(response.rows)
            page, pages = response.page, response.pages
            logging.debug(f"Processed page {page} of {pages}")
            params["page"] = page + 1
        for row in rows:
            _strip_id(row)
        if response.last_updated is None:
            return rows, None
        return rows, dt.datetime.strptime(response.last_updated, "%Y-%m-%d")
