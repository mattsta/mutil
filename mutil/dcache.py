""" Disk-based cache utils """

from dataclasses import dataclass, field
from typing import *

import os
import pathlib

import time
import arrow  # type: ignore

from loguru import logger


@dataclass
class FetchCache:
    """Given a URL and a cache key, cache the URL for either a day at most or
    as short as 'refreshMinutes'

    To properly use the cache feature, a new class instance used for each
    request instead of saving the result long term.

    e.g. await FetchCache(session, url, filename, refreshMinutes=20).get()
    """

    # An existing aiohttp.Session() already setup on this event loop.
    session: Any

    url: str
    filename: str
    refreshMinutes: Optional[int] = None

    # optional headers for authentication / overrides
    headers: Optional[dict[str, str]] = None
    params: Optional[dict[str, str]] = None  # query params
    filepath: Optional[str] = None
    cacheDir: Union[pathlib.Path, str] = field(
        default_factory=lambda: pathlib.Path(os.path.abspath(os.getcwd())).with_name(
            ".cache"
        )
    )

    def __post_init__(self):
        os.makedirs(self.cacheDir, exist_ok=True)

        # refresh a minimum of once per day
        fullFilename = self.filename + f"-{arrow.now().to('US/Eastern').date()}"
        if self.refreshMinutes is not None:
            # if refresh is requested, add a per-refreshMinutes bucket to
            # the filename for aggregating lookups
            fullFilename += f"-{int(time.time() / (self.refreshMinutes * 60))}"

        self.filepath = self.cacheDir / fullFilename
        logger.trace(f"Using cache path: {self.filepath} {self}")

    async def get(self):
        """If file already retrieved today, return cached value"""
        if os.path.isfile(self.filepath):
            logger.opt(depth=1).info(
                f"Returning previously cached path for {self.filename} at {self.filepath}"
            )

            # TODO: change to diskcache module?
            with open(self.filepath, "r") as f:
                return f.read()

        logger.opt(depth=1).info(
            f"Fetching {self.filename} from {self.url} into {self.filepath}"
        )
        got = await self.session.get(self.url, headers=self.headers, params=self.params)
        content = await got.text()

        # TODO: change to diskcache module?
        with open(self.filepath, "w") as outfile:
            outfile.write(content)

        return content
