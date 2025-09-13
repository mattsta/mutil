"""Pandas Utilities"""

import shutil

import pandas as pd  # type: ignore
from loguru import logger


def setupLogger():
    """Configure loguru logger to not format FRAME log levels.

    TODO: this could be enabled automatically if printFrame throws
    an exception for not having the FRAME log level... or we could
    just use it manually... or it could be auto-run on module import?"""
    logger.level("FRAME", no=25)


def printFrame(df, caption: str = "", headers: int | None = None, level: str = "FRAME"):
    """Print pandas dataframe to logger.

    Output can be prefixed with a description using 'caption'

    Also output can repeat the headers every N rows specified by 'headers'
    (default: only print headers at top of frame)

    By default, frame output is sent to a custom "FRAME" log level (to disable the default
    "enhanced bold" formatting on INFO output), which needs to be created as:
        logger.level("FRAME", no=25)

    If you want to log to the regular (or another level) output, use level='INFO'."""

    # Look up terminal size with each call so if user resizes
    # terminal between calls we always draw good bounds
    tsize = shutil.get_terminal_size()
    width = tsize.columns

    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        width,
        "display.max_colwidth",
        None,
    ):
        totalRows = df.shape[0]

        # depth=1 to report the CALLING function/method, not this wrapper, in the log.
        logger.opt(depth=1).info(
            "{}Row count: {}", f"{caption} :: " if caption else "", totalRows
        )
        try:
            if headers:
                # Prints headers every N rows, but looks
                # weird if tables are long (one row ends up as
                # multiple wider header blocks before repeating a new
                # start of rows again...)
                step = headers
                for i in range(0, totalRows, step):
                    logger.opt(depth=1).log(level, f"\n{df[i : i + step]}")
            else:
                logger.opt(depth=1).log(level, f"\n{df}")
        except Exception:
            logger.exception("Got excepts?")
