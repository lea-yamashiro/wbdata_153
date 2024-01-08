import datetime as dt
import re
from typing import Any, Dict, List, Union

import dateparser

from .types import DateSpec

PATTERN_YEAR = re.compile("\d{4}")
PATTERN_MONTH = re.compile("\d{4}M\d{1,2}")
PATTERN_QUARTER = re.compile("\d{4}Q\d{1,2}")


def convert_year_to_datetime(datestr: str) -> dt.datetime:
    """return datetime.datetime object from %Y formatted string"""
    return dt.datetime.strptime(datestr, "%Y")


def convert_month_to_datetime(datestr: str) -> dt.datetime:
    """return datetime.datetime object from %YM%m formatted string"""
    split = datestr.split("M")
    return dt.datetime(int(split[0]), int(split[1]), 1)


def convert_quarter_to_datetime(datestr: str) -> dt.datetime:
    """
    return datetime.datetime object from %YQ%# formatted string, where # is
    the desired quarter
    """
    split = datestr.split("Q")
    quarter = int(split[1])
    month = quarter * 3 - 2
    return dt.datetime(int(split[0]), month, 1)


def convert_dates_to_datetime(data: List[Dict[str, Any]]) -> None:
    """Replace date strings in raw response with datetime objects."""
    first = data[0]["date"]
    if isinstance(first, dt.datetime):
        return
    if PATTERN_MONTH.match(first):
        converter = convert_month_to_datetime
    elif PATTERN_QUARTER.match(first):
        converter = convert_quarter_to_datetime
    else:
        converter = convert_year_to_datetime
    for datum in data:
        datum_date = datum["date"]
        if "MRV" in datum_date or "-" in datum_date:
            continue
        datum["date"] = converter(datum_date)


def data_date_to_str(data_date: dt.datetime, freq: str) -> str:
    """
    Convert data_date to the appropriate representation base on freq


    :data_date: A datetime.datetime object to be formatted
    :freq: One of 'Y' (year), 'M' (month) or 'Q' (quarter)

    """
    if freq == "Y":
        return data_date.strftime("%Y")
    if freq == "M":
        return data_date.strftime("%YM%m")
    if freq == "Q":
        return f"{data_date.year}Q{(data_date.month - 1) // 3 + 1}"
    raise ValueError(f"Unknown Frequency type: {freq}")


def parse_single_date(date: Union[str, dt.datetime]) -> dt.datetime:
    if isinstance(date, dt.datetime):
        return date
    if PATTERN_YEAR.match(date):
        return convert_year_to_datetime(date)
    if PATTERN_MONTH.match(date):
        return convert_month_to_datetime(date)
    if PATTERN_QUARTER.match(date):
        return convert_quarter_to_datetime(date)
    last_chance = dateparser.parse(date)
    if last_chance:
        return last_chance
    raise ValueError(f"Unable to parse date string {date}")


def datespec_to_arg(spec: DateSpec, freq) -> str:
    if isinstance(spec, tuple):
        return (
            f"{data_date_to_str(parse_single_date(spec[0]), freq)}"
            ":"
            f"{data_date_to_str(parse_single_date(spec[1]), freq)}"
        )
    return data_date_to_str(parse_single_date(spec), freq)
