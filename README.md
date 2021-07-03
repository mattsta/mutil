mutil: Matt's Python Utilities
==============================

A collection of missing Python features not big enough to be their own independent projects. Tools are focused around improving asyncio and data processing usability.

Each module is either a `major` platform/service system or a `minor` tool/utility feature.

| What | Importance | Description |
|------|------------|-------------|
[`aiopipe.py`](mutil/aiopipe.py) | `Major` | async unix pipes
[`aioserver.py`](mutil/aioserver.py) | `Major` | async equivalent of `multiprocessing.connection` with expanded functionality for sendfile file serving
[`dataflow.py`](mutil/dataflow.py) | `Major` | multi-stage fan in/out data processing hierarchy
[`foreverloop.py`](mutil/foreverloop.py) | `minor` | automatically re-schedule coroutines after they complete while still waiting for more results
[`safeLoop.py`](mutil/safeLoop.py) | `minor` | wrapper around Python's complicated async event loop setup/shutdown mechanics to cleanly wait for tasks to go away (perhaps not as useful anymore since `asyncio.run()` now exists since Python 3.7+)
[`timer.py`](mutil/timer.py) | `minor` | a clean context manager for timing sections of code
[`dispatch.py`](mutil/dispatch.py) | `Major` | full custom command line generation dispatching system with minimum command prefix matching and showing command line docs from pydoc comments in your implementation classes.
[`dcache.py`](mutil/dcache.py) | `minor` | TTL disk cache for saving web requests (using `aiohttp`) to disk then returning from disk cache if not expired, or fetching and saving to disk if non-existing or expired.
[`frame.py`](mutil/frame.py) | `minor` | pretty print a complete pandas dataframe to console using the current console width.
[`numeric.py`](mutil/numeric.py) | `minor` | number helpers. can round up/down to fixed intervals and also return strings of numbers formatted to a specific min/max number of decimal places. e.g. return 3.3 as 3.30 for price printing, or return 3.6642100003 as 3.6642 for most detailed stock price.
[`sqlshard.py`](mutil/sqlshard.py) | `minor` | dict/map like thing where keys are saved to disk using N sqlite shards. values are saved as compressed JSON and saved/loaded automatically. can also run queries against all shards with threads (e.g. returning the length of the sharded dict by asking all underlying sqlite dicts their size concurrently). based on some ideas from [`diskcache`](https://github.com/grantjenks/python-diskcache/) for distribution and efficient on-disk serialization.
[`ratelimit.py`](mutil/ratelimit.py) | `minor` | simple async blocker to stop processing until the next allowed time/rate interval. used to match continuous server-side rate limits, but isn't accurate if the server-side rate limit you are trying to match resets on fixed intervals (i.e. 100 requests per minute, but resets on exact minute boundaries instead of a sliding window/leaky bucket rate limit system).
[`wproxy.py`](mutil/wproxy.py) | `Major` | websocket pub/sub proxy for when you need to rebroadcast a single-source websocket stream to multiple clients each with their own subscriptions. Useful for when an upstream service only allows one connection per API key, but you have multiple internal clients each needing their own subscriptions.
[`s4lru.py`](mutil/s4lru.py) | `Major` | segmented LRU for maintaining LRU metadata and size information. Each datum in the LRU has its size provided by callers, then the eviction process is also over a caller-defined size max, so the LRU can be used to manage any abstract key+size threshold abstraction required.  Performs 10+ million LRU addition/update operations per second when used under pypy.
[`awsSignatureV4.py`](mutil/awsSignatureV4.py) | `minor` | a user friendly dataclass for generating AWS V4 signatures for anything using the AWS signature hmac format (S3, B2, any other AWS-mocked services). Based on a clean refactoring of the [example aws sig generation code](https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html).


## aiopipe

aiopipe allows async reading and writing of little-endian length-prefixed data.

encoded data can read/written as:

- JSON using `readPickle()/writePickle(x)` (`x` is converted to JSON, written to the pipe, then loaded on the reader)
- raw bytes using `readActual()/writeWithHeader(x)` (no data conversion, just bytes write/read with length prefix)

The number of prefix header bytes is configurable as `AsyncPIPE(headerBytes=n)`.

The pipe may also use an existing pipe file descriptor (example: if this process is forked from a parent with open pipes)
using `AsyncPIPE(readFromFd=fd)` — but note: the external fd must write the little-endian prefix length bytes with a pre-agreed-upon `headerBytes` size.

## aioserver

aioserver is an async version of `multiprocessing.connection` also with JSON send/receive and also multiple helpers for
efficiently reading and writing files across a binary network connection.

Connections are mutually authenticated using shared secrets using the `multiprocessing.connection` handshake process, but we are using longer ciphers here.

Servers can be one of: TCP, WebSockets, or Unix sockets.

aioserver uses the same length-prefixed byte sending format as AsyncPIPE, so you have the option to send/receive JSON for auto pickle/unpickle or you can send/receive bytes directly.

Since the aioserver session is async, writes are guarded by an async lock so multiple writers don't end up interleaving their writes when readers are already engaged in an existing prefix-length datastream read (note: there's no HTTP/2 style channel multiplexing, but sends are fast enough and the only blocking behavior would happen around sending large files).

Data can be read over the connection easily using an async generator:

```python
async for dataSize, chunk in client.readBytesStreaming():
    print(dataSize, chunk)
```

Browse the [`PickleCxn` class](mutil/aioserver.py) for more details of client/server usage.

## foreverloop

foreverloop solves a common with python async handling where you have a collection of coroutines you want to run, but when a coroutine returns a result, you want to re-schedule the same coroutine again (example: async wait to read from network, receive network data, re-schedule network reading again).

Just give foreverloop the coroutines you want to run forever then use async generator `runForever()` to retrieve the awaitable result, and when you iterate again, the generator re-schedules the coroutine as a task and resumes yielding the next available result.


## safeLoop

safeLoop is a wrapper around python `run_forever()`/`run_until_complete()` with automatic task cancel on ctrl-c, etc. There are other more well developed projects doing the same, but this is a quick helper to avoid needing to pull in yet another project for some simple cases.

Run as:

```python
safeLoop.safeLoop(mainfn)
```

By default, safeLoop attempts to use `uvloop` if available, but if not available (or if manually overridden with `safeLoop.safeLoop(mainfn, customloop=None)`, falls back to `asyncio` loop (the difference was necessary because `uvloop` was catastrophically breaking when used under `prompt-toolkit`).

Unless you need the explicit auto-loop-override or safe cancellation mechanics, using modern `asyncio.run()` may be cleaner now.

## DataFlow

`dataflow.py` allows multi-hop data processing across various network and system topologies with each hop
being able to manipulate the data before passing it on.

Each level runs in its own forked Python process and the levels communicate over unix pipes.

For example, here's a simple echo server (also see [`./demo/echo.py`](demo/echo.py)):

- start one WebSocket server to read inbound numbers
- process the inbound numbers on 64 worker processes
- start two WebSocket servers to reply to outbound numbers
    - one server replies with even numbers
    - the other server replies with odd numbers

```python
    Inbound = [
        dataflow.WebSocketCxn(
            host="127.0.0.1",
            port=2221,
            mode=dataflow.NetworkMode.Listen,
            forwardFromClient=inboundForward,
        )
    ]

    Outbound = [
        dataflow.WebSocketCxn(
            host="127.0.0.1",
            port=2222,
            mode=dataflow.NetworkMode.Listen,
            forwardFromInternal=processingReply,
            filter=lambda x: x % 2 != 0,
        ),
        dataflow.WebSocketCxn(
            host="127.0.0.1",
            port=2223,
            mode=dataflow.NetworkMode.Listen,
            forwardFromInternal=processingReply,
            filter=lambda x: x % 2 == 0,
        ),
    ]

    topology = [
        dataflow.NetworkRunner(networks=Inbound),
        dataflow.Runner(target=processingLast, howMany=64),
        dataflow.NetworkRunner(networks=Outbound),
    ]

    dflow = dataflow.Dataflow(
        name="echo server!",
        topology=topology,
    )

    dflow.start()
```


Here's an example of reading from inbound unix pipes (launches `len(pipes)` worker processes),
doing [distributed data processing on the inbound pipe workers](https://matt.sh/load-balance-trades),
then forwarding results of the data processing to one output WebSocket for centralized retrieval:

```python
    Outbound = [
        # Sends bars to clients
        dataflow.WebSocketCxn(
            host="127.0.0.1",
            port=4442,
            mode=dataflow.NetworkMode.Listen,
            forwardFromInternal=processingReply,
        ),

        # Sends time-based stats to clients
        dataflow.WebSocketCxn(
            host="127.0.0.1",
            port=4443,
            mode=dataflow.NetworkMode.Listen,
            startup=aggregationSetup,
            forwardFromInternal=processingReplyForAggregations,
        ),
    ]

    topology = [
        # Bar workers run bars and output updates the bar analyzer subscribes to
        dataflow.Runner(target=barCreator, startup=barStartup, readPipes=pipes,),

        # Output where the bar analyzer subscribes to generated bars
        dataflow.NetworkRunner(networks=Outbound),
    ]

    dflow = dataflow.Dataflow(
        name="market activity forwarding server!", topology=topology
    )

    dflow.start()
```

## Timer

A useful utility for timing sections of code with a simple context manager.

Usage:

```python
from mutil.timer import Timer

with Timer(f"[{worker}] Finished Counting!"):
    for thing in things:
        count(thing)
```

## Frame

Print a pandas dataframe nicely to the terminal

```python
from loguru import logger
from mutil.frame import printFrame, setupLogger

# create the loguru log level (could be done manually elsewhere too)
# custom log level needed so loguru doesn't print the entire table
# in its default NEON BOLD WHITE log format.
setupLogger()

# print frame!
printFrame(df)

# print frame with caption!
printFrame(df, "optional caption for the log line")

# print frame with caption and repeat headers every 12 lines!
printFrame(df, "optional caption for the log line", 12)

# print frame with caption and repeat headers every 12 lines and
# log to different log level (defaults to 'FRAME' level)!
printFrame(df, "optional caption for the log line", 12, "INFO")
```

## numeric

### rounding

Includes functions to round decimal numbers to fixed intervals such as "round to nearest 0.05" or "round to nearest 0.25" etc.

```python
import mutil.numeric as n

def roundnear7(p, updown):
    """ Rounds near a *multiple* of 0.07, which may not be a number ending in .07"""
    return n.roundnear(0.07, p, updown)


def test_rn7_up():
    """ Round up """
    assert roundnear7(0.24, True) == 0.28
    assert roundnear7(0.01, True) == 0.07
    assert roundnear7(-0.01, True) == 0.00


def test_rn7_down():
    """ Round down """
    assert roundnear7(0.24, False) == 0.21
    assert roundnear7(0.01, False) == 0.00
    assert roundnear7(-0.01, False) == -0.07
```

### formatting

Print numbers as money values (expected use: stock prices) to a minimum of 2 decimals and a maximum of 4 decimals.

```python
from mutil.numeric import fmtPrice, fmtPricePad

assert fmtPrice(3.333333333333) == "3.3333"
assert fmtPrice(800000222222.333777777777777) == "800,000,222,222.3337"

# optional padding for right-alignment allowed via 'fmtPricePad()':
assert fmtPricePad(3.333333333333, 10) == "    3.3333"
assert fmtPricePad(800000222222.333777777777777, 30) == "        800,000,222,222.3337"
```

## sqlshard

A simple persistent dict implemented as sharded SQLite DBs under the hood.

Use case would be for storing potentially millions of keys in a dictionary and having the dictionary survive process restarts.

Keys are expected to be structured as `(key, timestamp, extradata)` so they can shard on 'key' but you can prune keys by timestamp in the future if needed. You can change the length of the dict key and the position of the shard key in the dict key with parameters `keylen` and `shardidx`.

Values are stored as zlib compressed JSON which is the smallest and fastest method of serializing python data structures on disk.

```python
from mutil.sqlshard import SQLShard
from pathlib import Path

# Will create 8 SQLite dicts in directory /tmp/bighash expecting keys
# to be 3-tuples with the first tuple index being the shard key.
s = SQLShard(shards=8, path=Path("/tmp/bighash", keylen=3, shardidx=0)

s[(1, 2, 3)] = dict(can="have", complex=("structures", 2, 3, ["and", "more"]))
```

## s4lru

High performance LRU (10x faster under pypy than cpython) for abstracting an LRU mechanism from its underlying access and eviction mechanics.

Example usage:

```python

from mutil.s4lru import S4LRU

# Create LRU with name and total max size (based on size of all individual items).
# You may also specify an optional third parameter adjusting the number of levels
# in the S4LRU to, for example, a 10 level LRU instead of the default 4 levels.
s = S4LRU("TestingLRU", 300)

# Tell LRU a key is accessed and also tell LRU the size of each key.
# - if a key is NEW, its LRU entry is immediately written to disk so it isn't lost.
# - if a key is just UPDATED, it is only updated in memory until manually flushed to disk.
s.increment("key1", 5)
s.increment("key2", 63)
s.increment("key3", 100)

# Manually flush to disk all in-memory access increments
s.flushCounters()

# Check for LRU being over size and needing evictions.
while s.evictCheck():
    # 'evicted' will be a list of LRU keys the S4LRU evicted.
    # Note: the LRU only returns the keys it deleted, but it is up
    #       to your application to actually delete/purge/clear the
    #       underlying data matching the evicted key (since S4LRU only
    #       manages the LRU by (key, size) data and knows *nothing* about
    #       the values being evicted themselves).
    evicted = s.evict()
    s.repr("LRU State Now")
```

LRU state (consisting of item keys, their sizes, and current cache level placement) is persisted to disk using sqlite and is automatically re-loaded when an LRU with the same name is requested on startup.

## awsSignatureV4

An easy to use modern dataclass for AWS V4 signature generation. Signatures have an expiration time component so do not cache them for future use.

```python

from mutil.awsSignatureV4 import SignatureV4

sig = SignatureV4(access_key, secret_key, "my-bucket.s3.us-west-001.amazonaws.com")
request_headers_dict = sig.getHeaders("GET", "/path-inside/some-bucket")

# Can be automated further with yarl
import yarl

def sig_for(access_key, secret_key, url):
    parts = yarl.URL(url)
    sig = SignatureV4(access_key, secret_key, parts.host)
    return sig.getHeaders("GET", parts.path)

request_headers_dict = sig_for(access_key, secret_key, "https://my-bucket.s3.us-west-001.amazonaws.com/path-inside/some-bucket")
```

The returned headers dict is a standard `Dict[str, str]` which can be used for sending headers in any python web fetching library so your request will be authorized by your API provider.


## Status
Most components of this project are in production and will receive updates as necessary.

Compatibility not guaranteed across releases. Pin to versions or even specific commit hashes if necessary.

Complete documentation and usage examples are sparse currently, but code should be easy to read with useful naming conventions.

Project currently passes `mypy` without strict and we would like to keep all updates
in the same type passing condition (errors about missing import types is okay).

Code formatting is standard `black` defaults.

Tests? Some tests. Maybe more tests in the future. Valid behavior generally verified by usage in other high throughput applications which would be failing if these libraries were broken.

Python version for some components is fixed to 3.7 because we are running under pypy for some installations. This means no nice `while got := await next(): got.process()`, etc. Also no usage of `Final[]` type annotations yet.
