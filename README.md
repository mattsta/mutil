mutil: Matt's Python Utilities
==============================

A collection of missing Python features not big enough to be their own independent projects. Tools are focused around improving asyncio and data processing usability.


Includes:

- `aiopipe.py` — async unix pipes
- `aioserver.py` — async equivalent of multiprocessing.connection with expanded functionality for file serving
- `dataflow.py` — multi-stage fan in/out data processing hierarchy
- `foreverloop.py` — automatically re-schedule coroutines after they complete while still waiting for more results
- `safeLoop.py` — wrapper around Python's complicated async event loop shutdown mechanics to cleanly wait for tasks to go away

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

Just give foreverloop the coroutines you want to run forever then use async generator `runForever()` to retrieve
the awaitable result, and when you iterate again, the generator re-schedules the coroutine as a task and resumes
yielding the next available result.


## safeLoop

safeLoop is a wrapper around python `run_forever()`/`run_until_complete()` with automatic task cancel on ctrl-c, etc. There are other more well developed projects doing the same, but this is a quick helper to avoid needing to pull in yet another project for some simple cases.

Run as:

```python
safeLoop.safeLoop(mainfn)
```

By default, safeLoop attempts to use `uvloop` if available, but if not available (or if manually overriden with `safeLoop.safeLoop(mainfn, customloop=None)`, falls back to `asyncio` loop (the difference was necessary because `uvloop` was catastrophically breaking when used under `prompt-toolkit`).


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


## Status
Project is in production and will receive updates as necessary.

Compatibility not guaranteed across releases. Pin to versions if necessary.

Complete documentation and usage examples are sparse currently, but code should be easy to read with useful naming conventions.

Project currently passes `mypy .` without strict and we would like to keep all updates
in the same type passing condition (errors about missing import types is okay).

Code formatting is standard `black` defaults.

Tests? No tests. Awful, right? Much of this was exploratory programming which got integrated into
other systems, so for now we know the parts work when we interop with our other platforms. We'll
pay down the technical debt one day.

Python version for this project is currently 3.7 because we are running under pypy for some installations. This means no nice `while got := await next(): got.process()`, etc. Also no usage of `Final[]` type annotations yet.

and yeah, we camelCase all our variable and function names. It's not the end of the world, you pep-8 fans.
