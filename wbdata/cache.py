import datetime as dt
import logging
import os
from pathlib import Path
from typing import Union

import appdirs
import cachetools
import shelved_cache  # type: ignore[import-untyped]

from wbdata import __version__

log = logging.getLogger(__name__)

try:
    TTL_DAYS = int(os.getenv("WBDATA_CACHE_TTL_DAYS", "7"))
except ValueError:
    logging.warning("Couldn't parse WBDATA_CACHE_TTL_DAYS value, defaulting to 7")
    TTL_DAYS = 7

try:
    MAX_SIZE = int(os.getenv("WBDATA_CACHE_MAX_SIZE", "100"))
except ValueError:
    logging.warning("Couldn't parse WBDATA_CACHE_MAX_SIZE value, defaulting to 100")
    MAX_SIZE = 7


def get_cache(
    path: Union[str, Path, None] = None,
    ttl_days: Union[int, None] = None,
    max_size: Union[int, None] = None,
) -> cachetools.Cache:
    """
    Get the global cache
    """
    path = path or Path(
        appdirs.user_cache_dir(appname="wbdata", version=__version__)
    ).joinpath("cache")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    ttl_days = ttl_days or TTL_DAYS
    max_size = max_size or MAX_SIZE
    cache: cachetools.TTLCache = shelved_cache.PersistentCache(
        cachetools.TTLCache,
        filename=str(path),
        maxsize=max_size,
        ttl=dt.timedelta(days=ttl_days),
        timer=dt.datetime.now,
    )
    cache.expire()
    return cache
