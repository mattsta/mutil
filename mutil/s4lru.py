#!/usr/bin/env python3

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Any, Union, Optional, Callable
import typing

import asyncio
from collections import Counter

from threading import Lock

# Note: mypy isn't happy with this decorator or how we use it on
#       CacheNode, so ignore the mypy errors for slotted_dataclass and
#       ignore for all the "unexpected keyword argument" on CacheNode.
def slotted_dataclass(dataclass_arguments=None, **kwargs):
    """Convert class fields to attrs for more efficient space usage."""
    if dataclass_arguments is None:
        dataclass_arguments = {}

    def decorator(cls):
        old_attrs = {}

        for key, value in kwargs.items():
            old_attrs[key] = getattr(cls, key)
            setattr(cls, key, value)

        cls = dataclass(cls, **dataclass_arguments)
        for key, value in old_attrs.items():
            setattr(cls, key, value)
        return cls

    return decorator


# replace with @dataclass(slots=True) for Python >= 3.10
# but as of right now, pypy is still on max 3.8, so it
# may be a while until we can use 3.10 features under
# pypy jit'ing.
@slotted_dataclass({"frozen": False})
class CacheNode:
    """Each cache entry is represented by one CacheNode.

    This is a high cardinality object.

    If you store 5 million elements in your LRU, you will
    have 4 million CacheNode objects.
    """

    __slots__ = ("prev", "next", "data", "level", "size", "key")
    prev: Optional[CacheNode]
    next: Optional[CacheNode]

    # Each node must know its own level, size, and name because:
    # when upgrading entries, we need to know the current level+size
    # when expiring entries, we need to know the current size+key
    level: int
    size: int
    key: str

    def append(self, new: CacheNode) -> None:
        """Append 'new' to current node (must be tail)"""
        assert not self.next
        self.next = new
        new.prev = self

    def prepend(self, new: CacheNode) -> None:
        """Prepend 'new' to current node (must be head)"""
        assert not self.prev
        assert not new.prev
        assert not new.next
        self.prev = new
        new.next = self

    def remove(self) -> None:
        """Remove current node from anywhere"""

        # [prev] [self] [next]
        # [prev] [next]
        # prev->next = self->next
        # next->prev = self->prev
        if self.prev:
            self.prev.next = self.next

        if self.next:
            self.next.prev = self.prev

        # node is removed from all links, to it has no peers
        self.prev = None
        self.next = None


@dataclass
class CacheLevel:
    """Each cache is represented by N levels (usually 4-6)

    This is a fixed cardinality object (per active LRU).
    """

    prev: Optional["CacheLevel"] = None
    next: Optional["CacheLevel"] = None
    head: Optional[CacheNode] = None
    tail: Optional[CacheNode] = None
    level: int = 0
    size: int = 0
    count: int = 0

    # Most node operations should be performed through
    # the level holding the node so the level can
    # maintain accurate 'head' and 'tail' pointers.
    def appendTail(self, key: str, size: int) -> CacheNode:
        new = CacheNode(prev=None, next=None, key=key, size=size, level=self.level)  # type: ignore
        self.size += size
        self.count += 1
        if self.tail:
            self.tail.append(new)

        if not self.head:
            self.head = new

        self.tail = new

        return new

    def removeForMove(self, mv: CacheNode) -> None:
        """Remove node 'mv' from this level.

        If node is the current head, use next node
        to be the new head.

        if node is the current tail, use prev node
        to be the new tail.
        """
        if mv is self.head:
            self.head = self.head.next

        if mv is self.tail:
            self.tail = self.tail.prev

        self.size -= mv.size
        self.count -= 1
        mv.remove()

    def prependHead(self, key: str, size: int) -> CacheNode:
        new = CacheNode(prev=None, next=None, key=key, size=size, level=self.level)  # type: ignore
        self.prependHeadNode(new)
        return new

    def prependHeadNode(self, new: CacheNode) -> None:
        """Move existing node 'new' to be head of this level."""
        self.size += new.size
        self.count += 1
        new.level = self.level

        if self.head:
            self.head.prepend(new)

        if not self.tail:
            self.tail = new

        self.head = new

    def removeTail(self) -> Optional[CacheNode]:
        if self.tail:
            orig = self.tail
            self.size -= orig.size
            self.count -= 1

            if self.tail.prev:
                self.tail = self.tail.prev
            else:
                self.tail = None
                self.head = None

            orig.remove()
            return orig

        return None

    def evict(self, targetCapacity: int) -> List[CacheNode]:
        """If level is over capacity, remove tail until under capacity"""
        removed = []
        if self.size > targetCapacity:
            while self.size > targetCapacity:
                old: CacheNode = self.removeTail()  # type: ignore
                if self.prev:
                    # if there's a prev level, move datum down
                    # NOTE: this is increasing the size of the previous level,
                    #       so if previous level could need evictions too.
                    self.prev.prependHeadNode(old)
                else:
                    # else, we are at the lowest level so we remove completely
                    removed.append(old)

                    # split evictions so we don't block too long at once
                    if len(removed) > 25:
                        break

        return removed


@dataclass
class S4LRU:
    """Manage a split level LRU (default: 4 levels).

    A split level LRU is basically a rectified LFU/LRU
    where the frequency is capped at the number of levels
    and each level maintains its own LRU via datum promotion
    to either the next level or the front of the current level
    on each access.

    Primary operations are:
        - record cache datum hit (maintains 'LRU' property)
        - expire levels if over capacity

    LRU also self-persists using a sqlite schema.
    """

    name: str

    # total cache size in bytes (or whatever units you are using for size attributes) before evictions start
    capacity: int
    levelCount: int = 4
    itemNode: Dict[str, CacheNode] = field(default_factory=dict)

    size: int = 0  # current size of all LRU content items
    capacityPerLevel: int = 0
    keyCounter: typing.Counter[str] = field(default_factory=lambda: Counter())

    def __post_init__(self) -> None:
        self.levels = [CacheLevel(level=x) for x in range(self.levelCount)]
        # tail is level 0, head is the highest level number
        # (and since we are range counting from 0 to N, the last level is the head)

        # just cache the zero-based highest index
        self.highestLevel = self.levelCount - 1

        # all post-init read/write/transactions need to be locked because we
        # often use the LRU from thread pool executors and sqlite3 can't handle
        # write concurrency across threads sharing one connection.
        self.lock = Lock()

        self.state = sqlite3.connect(
            f"sn4lru-{self.name}.sqlite", isolation_level=None, check_same_thread=False
        )
        self.cursor = self.state.cursor()
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS lru (key TEXT PRIMARY KEY, size INT, level INT, count INT)"
        )

        # S4LRU paper recommends giving each level equal size
        self.capacityPerLevel = self.capacity // self.levelCount

        # establish level hierarchy
        for level in self.levels:
            if level.level > 0:
                level.prev = self.levels[level.level - 1]

            if level.level < len(self.levels) - 1:
                level.next = self.levels[level.level + 1]

        # Repopulate LRU levels from DB state if exists.
        # We don't reproduce the levels *exactly*, but we do reproduce each level
        # with the *content* it had before, but the orders may be slightly off.
        # We are repopulating each level from least to most used, as recorded by the 'count'
        # field in the DB.
        for row in self.cursor.execute(
            "SELECT key, size, level FROM lru ORDER BY level, count"
        ):
            rkey: str = row[0]
            rsize: int = row[1]
            rlevel: int = row[2]
            # print(f"repopulating from {key} {size} {level}")
            # the min() check is in case the saved state has more levels
            # than the state being restored into.
            node = self.levels[min(self.highestLevel, rlevel)].prependHead(rkey, rsize)
            self.itemNode[rkey] = node
            self.size += rsize

    def evictCheck(self) -> bool:
        # Note: this is a rough check and doesn't account for
        #       some levels being overweight and technically needing
        #       eviction on their own if there's a level imblance
        #       but the overall LRU still being under the limit.
        return self.size > self.capacity

    @property
    def count(self) -> int:
        """Return count of all items in all levels.

        Each level maintains its own count, so the global count
        is a quick sum.
        """
        return sum([x.count for x in self.levels])

    def evict(self) -> List[str]:
        """Based on saved size calculations, clean up the LRU in a loop.

        Note: we iterate the levels from lowest to highest, so multiple
        loops are required to fully flush all levels if more than the lowest
        level is beyond its capacity.

        TODO: Make boosted evict so large items at high levels have more weight
              to drop through multiple previous levels instead of decaying
              one level at a time? (i.e. evict to lower levels with the inverse
              of the weight it took to elevate them higher, so large keys
              in the cache will fall out faster than smaller keys?)

        """

        # TODO: make this more size-aware by:
        #   - scanning the lowest 50 keys
        #   - sorting by size from largest to smallest
        #   - start deleting the largest files until either:
        #       - under capacity or
        #       - all files gone,
        #       - then repeat with next 50
        remove = []
        # evict anything required...
        with self.lock:
            for level in self.levels:
                removed: List[CacheNode] = level.evict(self.capacityPerLevel)

                if removed:
                    self.cursor.execute("BEGIN")
                    for r in removed:
                        remove.append(r.key)
                        self.size -= r.size
                        self.cursor.execute("DELETE FROM lru WHERE key = ?", (r.key,))

                    self.cursor.execute("COMMIT")

                    # break into removed chunks for iterative removal
                    if len(remove) > 25:
                        break

        return remove

    def flushCounters(self) -> None:
        """Flush our memory-cached key updates to disk then clear
        the memory key cache so it can continue accumulating updates.
        """
        with self.lock:
            self.cursor.execute("BEGIN")
            for key, count in self.keyCounter.items():
                # Do we really need to set 'level' here each time?
                # How would we restore the levels if we just had
                # the counts from highest to lowest? Problem though:
                # the counts don't take into effect temporal access,
                # just total access. I guess we could add a "last updated
                # timestamp" field then restore by "timestamp, count" sort
                # order, then split levels based on their max capacity in
                # a "re-balance but don't evict" capacity?

                try:
                    self.cursor.execute(
                        """WITH incr(x) AS
                            (SELECT count + ? FROM lru WHERE key=?)
                            UPDATE lru SET 
                                count = (SELECT x FROM incr),
                                level = ?
                            WHERE key=?""",
                        (
                            count,
                            key,
                            self.itemNode[key].level,
                            key,
                        ),
                    )
                except:
                    # if 'key' was already evicted from self.itemNode,
                    # don't throw an error just skip this update (because
                    # the key no longer exists)
                    pass

            self.cursor.execute("COMMIT")
        self.keyCounter.clear()

    def increment(self, key: str, size: int) -> None:
        """Insert or update 'key' inside LRU.

        If 'key' isn't currently in LRU, it is added.
        If 'key' is already in LRU, it is incremented.
        """
        node = self.itemNode.get(key)
        if node:
            # key already exists so we either need to:
            #   - if less than top level, move to head of next level
            #   - if at top level, move to head of level again

            # remove from current position
            # (we route this through the Level itself so the Level
            # can update the accounting size. Right now the level doesn't
            # know if we are keept it at the same level or moving to a higher
            # level; this could be optimized with a couple more checks to avoid
            # redundant subtraction followed by re-addition when just moving within
            # the highest level)
            self.levels[node.level].removeForMove(node)

            # use next level (or highest level if node+1 is too nigh)
            # within the bounds of [0, self.highestLevel]
            nextLevel = min(node.level + 1, self.highestLevel)
            targetLevel = self.levels[nextLevel]

            targetLevel.prependHeadNode(node)
            self.keyCounter[key] += 1
        else:
            # if key not in LRU, add as tail to lowest level:
            # self.size only increments here because this is a new insert
            # (the 'if' above just moves levels, so the overall size doesn't change)
            self.size += size
            node = self.levels[0].appendTail(key, size)
            self.itemNode[key] = node

            # technically this shouldn't need OR IGNORE because we should never
            # be double-adding a key to the LRU (existing key should be detected
            # in the previous branch and not re-added)
            with self.lock:
                self.cursor.execute(
                    "INSERT OR IGNORE INTO lru VALUES (?, ?, 0, 0)", (key, size)
                )

    def incrementBoost(self, key: str, size: int) -> None:
        """Insert or update 'key' inside LRU.

        Boosted increment lets smaller keys be more important than
        larger keys because to maintain LRU space, we should delete
        maybe slightly more used large files before we start deleting
        smaller files to reclaim space.

        Boost is currently managed by:
        If 'key' isn't currently in LRU, it is added.
        If 'key' is already in LRU, it is incremented.
        """
        node = self.itemNode.get(key)

        # for boosted increment:
        #  - under 1 MB jumps 4 levels
        #  - under 2 MB jumps 3 levels
        #  - under 3 MB jumps 2 levels
        #  - else jump one level

        # TODO: make boosting configurable
        if size < (1 << 20):
            levelBoost = 4
        elif size < 2 * (1 << 20):
            levelBoost = 3
        elif size < 3 * (1 << 20):
            levelBoost = 2
        else:
            levelBoost = 1

        if node:
            self.levels[node.level].removeForMove(node)

            # use next level (or highest level if node+1 is too nigh)
            # within the bounds of [0, self.highestLevel]
            nextLevel = min(node.level + levelBoost, self.highestLevel)
            targetLevel = self.levels[nextLevel]

            targetLevel.prependHeadNode(node)
            self.keyCounter[key] += 1
        else:
            # if key not in LRU, add as tail to lowest level:
            # self.size only increments here because this is a new insert
            # (the 'if' above just moves levels, so the overall size doesn't change)
            self.size += size
            startLevel = levelBoost - 1
            node = self.levels[startLevel].appendTail(key, size)
            self.itemNode[key] = node

            # technically this shouldn't need OR IGNORE because we should never
            # be double-adding a key to the LRU (existing key should be detected
            # in the previous branch and not re-added)
            with self.lock:
                self.cursor.execute(
                    "INSERT OR IGNORE INTO lru VALUES (?, ?, ?, 0)",
                    (key, size, startLevel),
                )

    def repr(self, msg: str = "", logger=print) -> None:
        """Print representation of LRU using 'logger' with optional header 'msg'.

        By default 'logger' is just 'print()' but if you are using loguru, you can
        set logger=logger.info, etc."""

        if msg:
            logger(f"============== {msg} ==============")

        logger(
            f"LRU {self.name} capacity {self.capacity:,}; size {self.size:,}; count {self.count:,}"
        )
        for idx, level in enumerate(self.levels):
            logger(f"[{idx}] size {level.size} count {level.count}")
            node = level.head
            while node:
                logger(f"[{idx}] {node.level} {node.size} {node.key}")
                node = node.next
