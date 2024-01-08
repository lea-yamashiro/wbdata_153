"""
wbdata: A wrapper for the World Bank API
"""
__version__ = "0.3.0.post"

import functools

from .client import Client


@functools.lru_cache
def get_default_client() -> Client:
    """
    Get the default client
    """
    return Client()


get_country = get_default_client().get_country
get_data = get_default_client().get_data
get_dataframe = get_default_client().get_dataframe
get_incomelevel = get_default_client().get_incomelevel
get_indicator = get_default_client().get_indicator
get_lendingtype = get_default_client().get_lendingtype
get_series = get_default_client().get_series
get_source = get_default_client().get_source
get_topic = get_default_client().get_topic
search_countries = get_default_client().search_countries
search_indicators = get_default_client().search_indicators
