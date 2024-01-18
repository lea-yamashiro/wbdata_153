import contextlib
import dataclasses
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Union

import decorator
import requests
import tabulate

from .dates import Dates

try:
    import pandas as pd  # type: ignore[import-untyped]
except ImportError:
    pd = None


from . import cache, dates, fetcher

BASE_URL = "https://api.worldbank.org/v2"
COUNTRIES_URL = f"{BASE_URL}/countries"
ILEVEL_URL = f"{BASE_URL}/incomeLevels"
INDICATOR_URL = f"{BASE_URL}/indicators"
LTYPE_URL = f"{BASE_URL}/lendingTypes"
SOURCES_URL = f"{BASE_URL}/sources"
TOPIC_URL = f"{BASE_URL}/topics"
INDIC_ERROR = "Cannot specify more than one of indicator, source, and topic"

DateArg = Union[Dates, None]
Id = Union[int, str]
IdArg = Union[Id, Sequence[Id], None]


class SearchResult(List[fetcher.Row]):
    """
    A list that prints out a user-friendly table when printed or returned on the
    command line


    Items are expected to be dict-like and have an "id" key and a "name" or
    "value" key
    """

    def __repr__(self) -> str:
        try:
            return tabulate.tabulate(
                [[o["id"], o["name"]] for o in self],
                headers=["id", "name"],
                tablefmt="simple",
            )
        except KeyError:
            return tabulate.tabulate(
                [[o["id"], o["value"]] for o in self],
                headers=["id", "value"],
                tablefmt="simple",
            )


if pd:

    class Series(pd.Series):
        """
        A pandas Series with a last_updated attribute
        """

        _metadata = ["last_updated"]

        @property
        def _constructor(self):
            return Series

    class DataFrame(pd.DataFrame):
        """
        A pandas DataFrame with a last_updated attribute
        """

        _metadata = ["last_updated"]

        @property
        def _constructor(self):
            return DataFrame
else:
    Series = Any  # type: ignore[misc, assignment]
    DataFrame = Any  # type: ignore[misc, assignment]


@decorator.decorator
def needs_pandas(f, *args, **kwargs):
    if pd is None:
        raise RuntimeError(f"{f.__name__} requires pandas")
    return f(*args, **kwargs)


def parse_value_or_iterable(arg: Any) -> str:
    """
    If arg is a single value, return it as a string; if an iterable, return a
    ;-joined string of all values
    """
    if isinstance(arg, str):
        return arg
    if isinstance(arg, Iterable):
        return ";".join(str(i) for i in arg)
    return str(arg)


def cast_float(value: str) -> Union[float, None]:
    """
    Return a value coerced to float or None
    """
    with contextlib.suppress(ValueError, TypeError):
        return float(value)
    return None


@dataclasses.dataclass
class Client:
    cache_path: Union[str, Path, None] = None
    cache_ttl_days: Union[int, None] = None
    cache_max_size: Union[int, None] = None
    session: Union[requests.Session, None] = None

    def __post_init__(self):
        self.fetcher = fetcher.Fetcher(
            cache=cache.get_cache(
                path=self.cache_path,
                ttl_days=self.cache_ttl_days,
                max_size=self.cache_max_size,
            )
        )
        self.has_pandas = pd is None

    def get_data(
        self,
        indicator: str,
        country: Union[str, Sequence[str]] = "all",
        date: DateArg = None,
        freq: str = "Y",
        source: IdArg = None,
        parse_dates: bool = False,
        skip_cache: bool = False,
    ):
        """
        Retrieve indicators for given countries and years

        :indicator: the desired indicator code
        :country: a country code, sequence of country codes, or "all" (default)
        :date: the desired date as a datetime object or a 2-tuple with start
            and end dates
        :freq: the desired periodicity of the data, one of 'Y' (yearly), 'M'
            (monthly), or 'Q' (quarterly). The indicator may or may not support the
            specified frequency.
        :source: the specific source to retrieve data from (defaults on API to 2,
            World Development Indicators)
        :parse_dates: if True, convert date field to a datetime.datetime object.
        :skip_cache: bypass the cache when downloading
        :returns: list of dictionaries
        """
        url = COUNTRIES_URL
        try:
            c_part = parse_value_or_iterable(country)
        except TypeError as e:
            raise TypeError("'country' must be a string or iterable'") from e
        url = "/".join((url, c_part, "indicators", indicator))
        args: Dict[str, Any] = {}
        if date:
            args["date"] = dates.format_dates(date, freq)
        if source:
            args["source"] = source
        data = self.fetcher.fetch(url, args, skip_cache=skip_cache)
        if parse_dates:
            dates.parse_row_dates(data)
        return data

    def _id_only_query(
        self,
        url: str,
        id_: Any,
        skip_cache: bool,
    ) -> SearchResult:
        """
        Retrieve information when ids are the only arguments

        :url: the base url to use for the query
        :id_: an id or sequence of ids
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects describing results
        """
        if id_:
            url = "/".join((url, parse_value_or_iterable(id_)))
        return SearchResult(self.fetcher.fetch(url=url, skip_cache=skip_cache)[0])

    def get_source(
        self, source_id: IdArg = None, skip_cache: bool = False
    ) -> SearchResult:
        """
        Retrieve information on a source

        :source_id: a source id or sequence thereof.  None returns all sources
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects describing selected
            sources
        """
        return self._id_only_query(
            url=SOURCES_URL, id_=source_id, skip_cache=skip_cache
        )

    def get_incomelevel(
        self, level_id: IdArg = None, skip_cache: bool = False
    ) -> SearchResult:
        """
        Retrieve information on an income level aggregate

        :level_id: a level id or sequence thereof.  None returns all income level
            aggregates
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects describing selected
            income level aggregates
        """
        return self._id_only_query(ILEVEL_URL, level_id, skip_cache=skip_cache)

    def get_topic(
        self, topic_id: IdArg = None, skip_cache: bool = False
    ) -> SearchResult:
        """
        Retrieve information on a topic

        :topic_id: a topic id or sequence thereof.  None returns all topics
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects describing selected
            topic aggregates
        """
        return self._id_only_query(TOPIC_URL, topic_id, skip_cache=skip_cache)

    def get_lendingtype(self, type_id=None, skip_cache=False):
        """
        Retrieve information on an income level aggregate

        :level_id: lending type id or sequence thereof.  None returns all lending
            type aggregates
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects describing selected
            topic aggregates
        """
        return self._id_only_query(LTYPE_URL, type_id, skip_cache=skip_cache)

    def get_country(
        self, country_id=None, incomelevel=None, lendingtype=None, skip_cache=False
    ):
        """
        Retrieve information on a country or regional aggregate.  Can specify
        either country_id, or the aggregates, but not both

        :country_id: a country id or sequence thereof. None returns all countries
            and aggregates.
        :incomelevel: desired incomelevel id or ids.
        :lendingtype: desired lendingtype id or ids.
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects representing each
            country
        """
        if country_id:
            if incomelevel or lendingtype:
                raise ValueError("Can't specify country_id and aggregates")
            return self._id_only_query(COUNTRIES_URL, country_id, skip_cache=skip_cache)
        args = {}
        if incomelevel:
            args["incomeLevel"] = parse_value_or_iterable(incomelevel)
        if lendingtype:
            args["lendingType"] = parse_value_or_iterable(lendingtype)
        return SearchResult(
            self.fetcher.fetch(url=COUNTRIES_URL, args=args, skip_cache=skip_cache)[0]
        )

    def get_indicator(self, indicator=None, source=None, topic=None, skip_cache=False):
        """
        Retrieve information about an indicator or indicators.  Only one of
        indicator, source, and topic can be specified.  Specifying none of the
        three will return all indicators.

        :indicator: an indicator code or sequence thereof
        :source: a source id or sequence thereof
        :topic: a topic id or sequence thereof
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects representing
            indicators
        """
        if indicator:
            if source or topic:
                raise ValueError(INDIC_ERROR)
            url = "/".join((INDICATOR_URL, parse_value_or_iterable(indicator)))
        elif source:
            if topic:
                raise ValueError(INDIC_ERROR)
            url = "/".join((SOURCES_URL, parse_value_or_iterable(source), "indicators"))
        elif topic:
            url = "/".join((TOPIC_URL, parse_value_or_iterable(topic), "indicators"))
        else:
            url = INDICATOR_URL
        return SearchResult(self.fetcher.fetch(url, skip_cache=skip_cache))

    def search_indicators(self, query, source=None, topic=None, skip_cache=False):
        """
        Search indicators for a certain regular expression.  Only one of source or
        topic can be specified. In interactive mode, will return None and print ids
        and names unless suppress_printing is True.

        :query: the term to match against indicator names
        :source: if present, id of desired source
        :topic: if present, id of desired topic
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects representing search
            indicators
        """
        indicators = self.get_indicator(
            source=source, topic=topic, skip_cache=skip_cache
        )
        pattern = re.compile(query, re.IGNORECASE)
        return SearchResult(i for i in indicators if pattern.search(i["name"]))

    def search_countries(
        self, query, incomelevel=None, lendingtype=None, skip_cache=False
    ):
        """
        Search countries by name.  Very simple search.

        :query: the string to match against country names
        :incomelevel: if present, search only the matching incomelevel
        :lendingtype: if present, search only the matching lendingtype
        :skip_cache: bypass cache when downloading
        :returns: SearchResult containing dictionary objects representing
            countries
        """
        countries = self.get_country(
            incomelevel=incomelevel, lendingtype=lendingtype, skip_cache=skip_cache
        )
        pattern = re.compile(query, re.IGNORECASE)
        return SearchResult(i for i in countries if pattern.search(i["name"]))

    @needs_pandas
    def get_series(
        self,
        indicator: str,
        country: Union[str, Sequence[str]] = "all",
        date: DateArg = None,
        freq: str = "Y",
        source: IdArg = None,
        parse_dates: bool = False,
        column_name: str = "value",
        keep_levels: bool = False,
        skip_cache: bool = False,
    ) -> Series:
        """
        Retrieve indicators for given countries and years

        :indicator: the desired indicator code
        :country: a country code, sequence of country codes, or "all" (default)
        :date: the desired date as a datetime object or a 2-tuple with start
            and end dates
        :freq: the desired periodicity of the data, one of 'Y' (yearly), 'M'
            (monthly), or 'Q' (quarterly). The indicator may or may not support the
            specified frequency.
        :source: the specific source to retrieve data from (defaults on API to 2,
            World Development Indicators)
        :parse_dates: if True, convert date field to a datetime.datetime object.
        :column_name: the desired name for the pandas column
        :keep_levels: if True don't reduce the number of index
            levels returned if only getting one date or country
        :skip_cache: bypass the cache when downloading
        :returns: Series
        """
        raw_data = self.get_data(
            indicator=indicator,
            country=country,
            date=date,
            freq=freq,
            source=source,
            parse_dates=parse_dates,
            skip_cache=skip_cache,
        )
        df = pd.DataFrame(
            [[i["country"]["value"], i["date"], i["value"]] for i in raw_data],
            columns=["country", "date", column_name],
        )
        df[column_name] = df[column_name].map(cast_float)
        if not keep_levels and len(df["country"].unique()) == 1:
            df = df.set_index("date")
        elif not keep_levels and len(df["date"].unique()) == 1:
            df = df.set_index("country")
        else:
            df = df.set_index(["country", "date"])
        series = Series(df[column_name])
        series.last_updated = raw_data.last_updated
        return series

    @needs_pandas
    def get_dataframe(
        self,
        indicators: Dict[str, str],
        country="all",
        date=None,
        freq="Y",
        source=None,
        parse_dates=False,
        keep_levels=False,
        skip_cache: bool = False,
    ) -> DataFrame:
        """
        Convenience function to download a set of indicators and  merge them into a
            pandas DataFrame.  The index will be the same as if calls were made to
            get_data separately.

        :indicators: An dictionary where the keys are desired indicators and the
            values are the desired column names
        :country: a country code, sequence of country codes, or "all" (default)
        :date: the desired date as a datetime object or a 2-sequence with
            start and end dates
        :freq: the desired periodicity of the data, one of 'Y' (yearly), 'M'
            (monthly), or 'Q' (quarterly). The indicator may or may not support the
            specified frequency.
        :source: the specific source to retrieve data from (defaults on API to 2,
            World Development Indicators)
        :parse_dates: if True, convert date field to a datetime.datetime object.
        :keep_levels: if True don't reduce the number of index levels returned if
            only getting one date or country
        :skip_cache: bypass the cache when downloading
        :returns: a DataFrame
        """
        serieses = [
            (
                self.get_series(
                    indicator=indicator,
                    country=country,
                    date=date,
                    freq=freq,
                    source=source,
                    parse_dates=parse_dates,
                    keep_levels=keep_levels,
                    skip_cache=skip_cache,
                ).rename(name)
            )
            for indicator, name in indicators.items()
        ]
        result = None
        for series in serieses:
            if result is None:
                result = series.to_frame()
            else:
                result = result.join(series.to_frame(), how="outer")
        result = DataFrame(result)
        result.last_updated = {i.name: i.last_updated for i in serieses}
        return result
