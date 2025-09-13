"""Shard SQLite DBs by key but allow range searches"""

import concurrent.futures
import pathlib
import sqlite3
import zlib
from dataclasses import dataclass, field
from typing import Any, Sequence

import orjson

# TODO: implementing deleting by ranges...
# TODO: allow adjustable range column instead of defaulting to the second element


@dataclass
class SQLShard:
    """Use N shards for SQLite access while allowing global key searches.

    The default implementation assumes keys look like:
        (SHARD_KEY, TIMESTAMP, OTHER)

    So the SHARD_KEY picks the underlying DB while TIMESTAMP is a range column we can
    query using either equals, GT, LT, or GT AND LT operations."""

    shards: int = 16
    path: str | pathlib.Path = field(default_factory=pathlib.Path)
    keylen: int = 3
    shardidx: int = 0

    readstmt_EXACT: dict[int, str] = field(default_factory=dict)
    readstmt_LT: dict[int, str] = field(default_factory=dict)
    readstmt_GT: dict[int, str] = field(default_factory=dict)
    readstmt_GT_2: dict[int, str] = field(default_factory=dict)
    readstmt_GTLT: dict[int, str] = field(default_factory=dict)

    def __post_init__(self):
        """Create the DB dirs and mappings if they don't already exist."""

        keyDesc = ", ".join([f"key_{n}" for n in range(self.keylen)])

        schema = (
            f"CREATE TABLE IF NOT EXISTS db ({keyDesc}, data, PRIMARY KEY({keyDesc}))"
        )

        self.db = {}
        for n in range(self.shards):
            dbdir = pathlib.Path(self.path) / "sqlshard" / str(n)
            dbdir.mkdir(parents=True, exist_ok=True)
            self.db[n] = sqlite3.connect(
                dbdir / "sqlshard.db", isolation_level=None, check_same_thread=False
            )

        self.c = {k: v.cursor() for k, v in self.db.items()}

        for k, v in self.c.items():
            v.execute("PRAGMA journal_mode = WAL")
            v.execute(schema)

        # question marks for key (for generating insert statements)
        qfk = ", ".join(["?" for _ in range(self.keylen)])
        # insert key fields then data field
        self.insertstmt = f"INSERT OR REPLACE INTO db VALUES ({qfk}, ?)"

        def allSelect(keysDesc: Sequence[str]):
            keyDescExact = " AND ".join(keysDesc)
            return f"SELECT * FROM db WHERE {keyDescExact}"

        for n in range(self.keylen):
            keyDescParts = [f"key_{i} = ?" for i in range(n + 1)]
            self.readstmt_EXACT[n] = allSelect(keyDescParts)

            if n > 1:
                secondLT = keyDescParts.copy()
                secondGT = keyDescParts.copy()
                secondGTLT = keyDescParts.copy()

                secondLT[1] = "key_1 <= ?"
                secondGT[1] = "key_1 >= ?"
                secondGTLT[1] = "key_1 >= ? AND key_1 <= ?"
                self.readstmt_LT[n] = allSelect(secondLT)
                self.readstmt_GT[n] = allSelect(secondGT)
                self.readstmt_GTLT[n] = allSelect(secondGTLT)

    def cursorForKey(self, key: Sequence) -> sqlite3.Cursor:
        hashkey = zlib.adler32(key[self.shardidx]) % self.shards
        return self.c[hashkey]

    def rangeGT(self, partialKey: Sequence):
        c = self.cursorForKey(partialKey)
        got = c.execute(self.readstmt_GT[1], (partialKey[0], partialKey[1]))
        if got:
            return (
                (*x[0 : self.keylen], orjson.loads(zlib.decompress(x[-1]))) for x in got
            )

    def rangeGTLT(self, partialKey: Sequence, maxLT: Any):
        """Return a range scan result.

        Scan is conducted via GTE on the second index of the partial key then
        bounded LTE by the maxLT parameter.

        Result is a generator yielding the full row data including all key columns."""
        c = self.cursorForKey(partialKey)
        got = c.execute(self.readstmt_GTLT[0], (partialKey[0], partialKey[1], maxLT))
        if found := got.fetchall():
            return (
                (*x[0 : self.keylen], orjson.loads(zlib.decompress(x[-1])))
                for x in found
            )

    def __getitem__(self, key: Sequence) -> Any:
        c = self.cursorForKey(key)
        got = c.execute(self.readstmt_EXACT[self.keylen - 1], key)
        if found := got.fetchone():
            return orjson.loads(zlib.decompress(found[-1]))

        raise KeyError(key)

    def doall(self, stmt: str):
        """Run a query against all shards and return the result as a list"""

        def goodbye(shard, c):
            return c.execute(stmt).fetchone()

        result = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            doit = [executor.submit(goodbye, shard, c) for shard, c in self.c.items()]
            for future in concurrent.futures.as_completed(doit):
                result.append(future.result())

        return result

    def __len__(self):
        return sum([x[0] for x in self.doall("SELECT COUNT(*) FROM db")])

    def __setitem__(self, key: Sequence, val: Any):
        c = self.cursorForKey(key)
        c.execute(self.insertstmt, (*key, zlib.compress(orjson.dumps(val), 1)))

    def clear(self):
        self.doall("DELETE FROM db")
