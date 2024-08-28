import asyncio
import pathlib
import shutil

from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Mapping

import diskcache  # type: ignore
from loguru import logger


@dataclass(slots=True)
class DualCache:
    """A rapid lookup dual-layer cache.

    Goal: cache everything locally in memory while maintaining a persistent disk cache,
          but use the in-memory cache for all reads, but save writes to the disk cache
          so we can easily reload the entire previously saved cache on startup again.

    Why do we bother with this?

    We like using diskcache for persistence, but a diskcache lookup is 4-5 µs while a
    python dict in-memory lookup is 30 ns (15x faster).
    """

    cacheName: str

    # optionally place cache in a difference place than the current directory
    cachePrefix: str = ""

    cacheMem: dict[Any, Any] = field(init=False)
    cacheDisk: Mapping[Any, Any] = field(init=False)

    get: Callable = field(init=False)

    events: dict[Any, asyncio.Event] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.cacheName = self.cacheName.replace(" ", "-")
        self.cacheDisk = diskcache.Cache(
            (self.cachePrefix or "./cache-") + self.cacheName
        )
        self.get = self.cacheDisk.get

        # load disk cache into memory for faster immediate reads
        # (yes, this is the simplest way... diskcache doesn't have .keys() or .values())
        self.cacheMem = {k: self.cacheDisk[k] for k in self.cacheDisk.iterkeys()}  # type: ignore

    def destroy(self):
        """Delete the cache directory from disk"""
        assert self.cacheDisk.directory not in {
            "",
            "/",
            "/usr",
            "/tmp",
            str(pathlib.Path.home()),
        }
        shutil.rmtree(self.cacheDisk.directory)

    def __getitem__(self, who):
        # always read from memory
        return self.cacheMem.get(who)

    def __setitem__(self, who, what):
        # always set to memory _and_ disk
        self.cacheMem[who] = what
        self.cacheDisk[who] = what  # type: ignore

    def iterkeys(self):
        """match the diskcache API for iterating things"""
        yield from self.cacheMem.keys()

    def items(self):
        """match the dict API for iterating things"""
        yield from self.cacheMem.items()

    def keys(self):
        yield from self.cacheMem.keys()

    def values(self):
        yield from self.cacheMem.values()

    def clear(self):
        self.cacheMem.clear()
        self.cacheDisk.clear()

    def checkDisk(self, who):
        # Check if maybe it ended up on disk, restore to mem, then return it too
        if what := self.cacheDisk.get(who):
            self.cacheMem[who] = what
            return what

        # For now, we think just "forever setting" all data will be fine, but if we do need
        # to expire our sets, we can return to using this pattern:

        # just rotate all sets to expire in 14 days so we don't end up with _infinite_ stale
        # data, but this is long enough where performance won't be a problem when it refreshes.
        # self.cacheDisk.set(who, what, expire=86400 * 14)

    async def lock(self, key):
        # if we try to lock something twice (why? how?) just block on the lock itself.
        if event := self.events.get(key):
            await event.wait()
            return

        self.events[key] = asyncio.Event()

    async def locked(self, key):
        """Check if we have a lock for key. If so, wait until event is complete.

        If we don't have a lock, then the resource isn't protected and we just continue as normal.
        """
        if event := self.events.get(key):
            # logger.info("Waiting for {}", key)
            await event.wait()
            # logger.info("Unlocked for {}", key)

    def unlock(self, key):
        """Announce the key event is released and also delete it from pending events."""

        # only set if the event still exists. something else waiting may have already reached here first.
        if event := self.events.get(key):
            event.set()
            del self.events[key]
