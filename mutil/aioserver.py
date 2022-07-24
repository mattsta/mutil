""" aioservers for WebSockets, TCP, and Unix pipes.

Data transferred either by JSON encoding or direct byte sending.

Also has helper functions for writing directly into and out from
files using aiofiles and sendfile helpers."""


import os
import io
import hmac
import json
import fcntl
import asyncio
import aiofiles
import aiofiles.os

# for making temp names easily without asking the OS
import ulid  # type: ignore

import socket
import platform
import websockets

from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Tuple,
    List,
    Callable,
    Iterable,
    Optional,
    Any,
    Union,
    AsyncIterable,
    Awaitable,
)
from multiprocessing import AuthenticationError

from loguru import logger
from inspect import getmembers

if platform.system() == "Linux":
    isLinux = True
else:
    isLinux = False

# ugh, pypy added THEN REMOVED sendfile so we need to workaround
# os.sendfile not existing when using really old pypy or new pypy but
# not medium-old pypy.
VM_HAS_SENDFILE = hasattr(aiofiles.os, "sendfile")

# challenge / response adapted from multiprocessing.connection
# https://github.com/python/cpython/blob/a3a4bf3b8dc79e4ec4f24f59bd1e9e2a75229112/Lib/multiprocessing/connection.py#L727-L764
CHALLENGE = b"#CHALLENGE#"
WELCOME = b"#WELCOME#"
FAILURE = b"#FAILURE#"

assert len(WELCOME) == len(
    FAILURE
), "We need welcome and failure to be same length for the network processing..."

# 64 byte (512-bit) digests
HASH_ALGO = "blake2b"

# Give the hmac enough random bytes to mix down
# NOTE: do not use hmac.digest() because it isn't supported by the
#       current openssl libs (which .digest() just proxies into)
DIGEST_LEN = len(hmac.new(b"a", b"b", HASH_ALGO).digest())
RANDBYTES_LEN = DIGEST_LEN * 2

# Server sends challenge to clients as: #CHALLENGE#[randombytes]
# Client responds with 64-byte blake2b hmac(shared-key, challenge-from-server)
CHALLENGE_SEND_LEN = len(CHALLENGE) + RANDBYTES_LEN
CHALLENGE_REPLY_LEN = DIGEST_LEN


def flushBufferUpTo(self, size):
    """Helper for using sendfile on Linux to read socket into file directly"""

    blen = len(self._buffer)
    if blen == 0:
        return 0, None

    # else, buffer doesn't have as much data as we asked for, so move
    # the current buffer then clear it for future use
    if blen <= size:
        data = bytes(self._buffer)
        self._buffer.clear()
        return blen, data

    # else, buffer has MORE data than our flush request, so split it off
    # into our requested length plus shrink the remaining buffer for future
    # appending.
    data = bytes(self._buffer[:size])
    del self._buffer[:size]

    # according to the streamreader pattern, we should inform it we consumed
    # all the bytes so it can potentially un-pause itself...
    self._maybe_resume_transport()
    return size, data


asyncio.StreamReader.flushBufferUpTo = flushBufferUpTo  # type: ignore


class ConnectionClosedOK(Exception):
    """Exception for notifing caller an async generator reached socket EOF
    while waiting for the next frame."""

    pass


@dataclass
class PickleCxn:
    """Multi-transport frame encoding for setting up and communicating
    across a dual-authenticated connection."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    authKey: bytes = field(repr=False)
    writerfd: Optional[int] = None
    loads: Callable[..., Any] = json.loads
    dumps: Callable[..., bytes] = lambda x: json.dumps(x).encode()

    # Note: 'writeLock' is used to protect against multiple writes into one
    # frame while a previous frame is being awaited to write more data.
    # We don't allow interleaved data in our sending formats because we don't
    # split our larger frames into sub-frames, we just send one large frame,
    # expect it to be received in-order, then send the next.
    # Just locking all self.writer access prevents multiple async coroutine
    # writes being scheduled in conflict with the unified in-order datastream.
    writeLock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # you can customize the byte length for content size sending.
    # sizes are unsigned, so:
    #  1 = 255 bytes max
    #  2 = 64 KiB
    #  3 = 16 MiB
    #  4 = 4 GiB
    #  5 = 1 TiB
    #  6 = 256 TiB
    #  7 = 64 PiB
    #  8 = 16 EiB
    #  9 = 4 ZiB
    # 10 = 1 Yobibyte

    # If you attempt to send content larger than your maximum byte size,
    # python will throw an exception when it fails to convert a too-large
    # size into a too-small unsigned integer.

    # also for reference, in 1 second:
    # 500 Mbps =  ~60 MiB (0.060 MiB/ms)
    #   1 Gbps = ~120 MiB (0.12 MiB/ms)
    #   2 Gbps = ~240 MiB (~0.24 MiB/ms)
    #   5 Gbps = ~600 MiB (~0.60 MiB/ms)
    #  10 Gbps = ~1.12 GiB (~1.2 MiB/ms)
    #  40 Gbps = ~4.67 GiB (~4.77 MiB/ms)
    # 100 Gbps = ~11.64 GiB (~11.92 MiB/ms)
    # 200 Gbps = ~23.28 GiB etc
    # 300 Gbps = ~34.92 GiB
    # 400 Gbps = ~46.57 GiB
    #   1 Tbps = ~116.42 GiB
    #  10 Tbps = ~1.14 TiB (~1.164 GiB/ms)
    #  40 Tbps = ~4.55 TiB
    # 100 Tbps = ~11.34 TiB
    headerBytes: int = 4  # increase for > 4 GB transfers

    # ==========================================================================
    # Init
    # ==========================================================================
    def __post_init__(self) -> None:
        # save the writer fd so we can read disk sendfile to network
        writersock = self.writer.get_extra_info("socket")
        self.writerfd = writersock.fileno()

        # convert to help user if they gave us a string instead of bytes
        if isinstance(self.authKey, str):
            self.authKey = self.authKey.encode()

        if self.loads is None and self.dumps is None:
            try:
                import orjson

                self.loads = orjson.loads
                self.dumps = orjson.dumps
            except:
                # under pypy we won't have orjson, so use
                # the system-provided JSON
                self.loads = json.loads
                self.dumps = lambda x: json.dumps(x).encode()

    # ==========================================================================
    # Mutual Authentication Setup
    # ==========================================================================
    async def challengeSend(self) -> None:
        """Client sends challenge to server when client connects"""
        message = os.urandom(RANDBYTES_LEN)
        self.writer.write(CHALLENGE + message)
        await self.writer.drain()

        digest = hmac.new(self.authKey, message, HASH_ALGO).digest()

        response = await self.reader.readexactly(CHALLENGE_REPLY_LEN)
        if response == digest:
            self.writer.write(WELCOME)
            await self.writer.drain()
        else:
            self.writer.write(FAILURE)
            await self.writer.drain()
            raise AuthenticationError("digest received didn't verify?")

    async def challengeReply(self) -> None:
        """Server replies to client challenges"""
        message = await self.reader.readexactly(CHALLENGE_SEND_LEN)
        assert message[: len(CHALLENGE)] == CHALLENGE, f"message = {message!r}"
        message = message[len(CHALLENGE) :]

        digest = hmac.new(self.authKey, message, HASH_ALGO).digest()

        self.writer.write(digest)
        await self.writer.drain()
        response = await self.reader.readexactly(len(WELCOME))

        if response != WELCOME:
            raise AuthenticationError("digest sent was rejected")

    # ==========================================================================
    # Quick Lock Check
    # ==========================================================================
    def isWriteLocked(self):
        return self.writeLock.locked()

    # ==========================================================================
    # Header-Related Data Operations
    # ==========================================================================
    async def getHeaderSize(self) -> int:
        """Helper function to extract length of next data sent"""
        dataSizeAsEncodedBytes = await self.reader.readexactly(self.headerBytes)
        return int.from_bytes(dataSizeAsEncodedBytes, byteorder="little")

    async def writeEncodedBytes(self, b: bytes) -> None:
        """Write already 'self.headerBytes' prefix-encoded frames to the peer.

        This is useful for servers broadcasting to multiple clients at once
        so each new send doesn't have to re-encode the data.
        """
        async with self.writeLock:
            self.writer.write(b)
            await self.writer.drain()

    # ==========================================================================
    # Byte Writing Operations
    # ==========================================================================
    def tryWriteBytes(self, b: bytes) -> Awaitable[None]:
        """Only write bytes if not currently locked, else return.

        Useful for empty data sending like heartbeats, pings, etc."""

        if not self.isWriteLocked:
            return self.writeBytes(b)

        # else, return a no-op coroutine
        return asyncio.sleep(0)

    async def writeBytes(self, b: bytes) -> None:
        """Write raw bytes with a generated length prefix header.

        For use when orjson isn't available or you don't need
        JSON encoding/decoding on each end of the stream.
        """
        psize = len(b)
        header = psize.to_bytes(self.headerBytes, byteorder="little")
        async with self.writeLock:
            if psize > 16384:
                # if big, send two
                self.writer.write(header)
                self.writer.write(b)
            else:
                # if small, concat buffer saves an independent send
                self.writer.write(header + b)

            await self.writer.drain()

    # ==========================================================================
    # Byte Reading Operations
    # ==========================================================================
    async def readBytes(self) -> Optional[bytes]:
        """Return entire data record from stream using header-prefix length"""
        try:
            dataSizeAsEncodedBytes = await self.reader.readexactly(self.headerBytes)
        except asyncio.IncompleteReadError as e:
            if self.reader.at_eof():
                return None

        dataSize = int.from_bytes(dataSizeAsEncodedBytes, byteorder="little")
        return await self.reader.readexactly(dataSize)

    async def readBytesStreaming(self) -> AsyncIterable[Tuple[int, bytes]]:
        """async iterator for returning record data chunks until complete"""
        try:
            dataSizeAsEncodedBytes = await self.reader.readexactly(self.headerBytes)
        except asyncio.IncompleteReadError as e:
            # don't throw a noisy exception if this is a clean shutdown
            if self.reader.at_eof():
                raise ConnectionClosedOK

        dataSize = int.from_bytes(dataSizeAsEncodedBytes, byteorder="little")

        # if data size is under 1 MB, guarantee we return it all at once
        if dataSize < (1 << 20):
            yield dataSize, await self.reader.readexactly(dataSize)
        else:
            # else, chunk the reply so we don't have to load it all at once, but
            # still use minimum 1 MB chunks (except for final data completion)
            received = 0
            while received < dataSize:
                # read either 1 MB at a time or consume the remaining data
                got = await self.reader.readexactly(min(1 << 20, dataSize - received))
                received += len(got)
                yield dataSize, got

    # ==========================================================================
    # Whole File Receiving Operations
    # ==========================================================================
    async def readIntoFile(self, targetPath: Union[str, Path]) -> int:
        """Read next record into 'targetPath'

        We write into a temp file in the same directory as 'targetPath' first
        so any other users expecting the file won't be reading the file as
        it is written.

        If the complete record is received, we rename the tempfile to the target
        file before returning.

        Note: on Linux you can potentially use readIntoFileSendfile() to let
        Linux write directly into the file from the kernel without chunking
        it ourselves first.
        """
        # logger.info("Reading into {}...", targetPath)
        totalSize = 0
        tempFile = str(targetPath) + "." + str(ulid.new())
        try:
            # Create the directory for target file if it doesn't exist yet...
            dname = os.path.dirname(targetPath)
            if not os.path.isdir(dname):
                os.makedirs(dname, exist_ok=True)

            # logger.debug("Opening for temp reading... {}", tempFile)
            async with aiofiles.open(tempFile, "wb") as f:
                # logger.info("Opened file...")
                async for chunkSize, chunk in self.readBytesStreaming():
                    totalSize += await f.write(chunk)

            if totalSize > 0:
                # move tempfile to final good filename
                await aiofiles.os.rename(tempFile, targetPath)
            else:
                # else, didn't create anything, so delete tempfile
                await aiofiles.os.remove(tempFile)

            return totalSize
        except:
            # problem writing? remove tempfile.
            logger.exception(
                "Problem writing? totalSize: {} tmpFile: {}", totalSize, tempFile
            )
            try:
                await aiofiles.os.remove(tempFile)
            except:
                pass

            return 0

    async def readIntoFileSendfile(self, targetPath: Union[str, Path]) -> int:
        """Read next data bytes into a file directly from the network using
        sendfile().

        Modern Linux (since 2.6.33) supports any-fd-to-any-fd for sendfile while
        previous versions (and other operating systems) limit sendfile to 'out'
        being a socket and 'in' being a file.

        Note: uses our custom patched StreamReader helper to flush any existing
        buffer before doing the network-to-disk sendfile  (otherwise StreamReader
        may have over-read a previous request, be holding more data inside its private
        buffer, so it could have a partial complete file in the buffer before
        we ask the kernel to sendfile() the next 'dataSize' bytes to disk)
        """
        dataSizeAsEncodedBytes = await self.reader.readexactly(self.headerBytes)
        dataSize = int.from_bytes(dataSizeAsEncodedBytes, byteorder="little")

        # server signals failure by returning zero size with zero data
        if dataSize == 0:
            return 0

        # create parent directory if it doesn't exist...
        Path(targetPath).parent.mkdir(parents=True, exist_ok=True)

        # now attempt to write from the reader into the target file
        tempFile = str(targetPath) + "." + str(ulid.new())
        try:
            async with aiofiles.open(tempFile, "wb") as f:
                fd = f.fileno()

                remainingData = dataSize
                bufferSize, bufferBytes = self.reader.flushBufferUpTo(remainingData)

                if bufferSize:
                    f.write(bufferBytes)
                    f.flush()
                    remainingData -= bufferSize

                # we assume f.write() properly increments the file offset so below
                # '0' references the current fd position and not start-of-file.
                assert self.writerfd
                while remainingData > 0:
                    # .writerfd because there is no .readerfd on a bidirectional cxn
                    try:
                        wrote = await aiofiles.os.sendfile(
                            fd, self.writerfd, 0, remainingData
                        )
                    except (BlockingIOError, InterruptedError):
                        continue

                    if wrote == 0:
                        # if hit EOF (closed connection), but we didn't send all the data,
                        # it's an error.
                        # TODO: exception vs error return; caller can't tell how much was sent
                        if remainingData > 0:
                            # jumps to exception for removing the tempfile then returns 0
                            raise IOError(
                                (
                                    remainingData,
                                    f"Incomplete write? Got EOF with {remainingData} bytes remaining",
                                )
                            )

                        break

                    remainingData -= wrote

            # success! rename tempfile to expected final filename
            await aiofiles.os.rename(tempFile, targetPath)
            return dataSize
        except Exception as e:
            # problem writing? remove tempfile.
            await aiofiles.os.remove(tempFile)
            raise e

    # ==========================================================================
    # Whole File Sending Operations
    # ==========================================================================
    async def writeFromSendfile(self, filefd, totalSize=0) -> int:
        """Write entire 'filefd' to current stream

        Can optionally provide the known file size to prevent one extra sendfile read.
        (sendfile signals complete by returning '0' on the next call after all
         bytes have been written from 'filefd')

        Note: sendfile() not available under pypy on macOS for recent releases.
        """
        start = 0
        while True:
            # sendfile returns 0 when hitting EOF on 'filefd' (but returns bytes
            # sent before returning 0 when no more bytes are available)
            try:
                sent = await aiofiles.os.sendfile(self.writerfd, filefd, start, 0)  # type: ignore
            except (BlockingIOError, InterruptedError):
                continue
            except:
                logger.exception("Other error?")
                raise

            # logger.info("OS sendfile returned: {}", sent)

            # update total sent, which may also be the next start offset
            start += sent

            if start == totalSize or sent == 0:
                return sent

            # Just extra details if you're curious about where sendfile breaks
            # logger.warning(
            #    f"Didn't send complete file? Sent {start} bytes with total size: {totalSize}"
            # )

        return start
        # note: failure to reach end-of-file here may have corrupted our stream;
        #       we already sent our prefix length header, so any failed
        #       send will break the reader since those bytes won't be
        #       populated in the stream.

    async def writeFromSendfileRange(self, filefd: int, start: int, end: int) -> None:
        """Write 'filefd' to current stream from 'start' to 'end' bytes"""
        logger.debug("Sending sendfile: {} {} {}", filefd, start, end)
        remainingBytes = end
        assert self.writerfd
        while remainingBytes > 0:
            # TODO: instead of maintaining 'end' we could also just use 'end' == 0
            #       then the kernel sends 'filefd' until it hits EOF, but we still
            #       have to loop until we get a return value of 0 for a complete send.
            # sendfile returns bytes written then 0 on EOF
            sent = await aiofiles.os.sendfile(
                self.writerfd, filefd, start, remainingBytes
            )

            logger.debug("OS sendfile returned: {}", sent)
            if sent == 0:
                break

            remainingBytes -= sent

            # and if we are looping again, update next start position...
            start += sent

            logger.warning(
                f"Didn't send complete file? Sent {start} out of {end} bytes"
            )
        # note: failure here may have corrupted our stream because
        #       we already sent our prefix length header, so any failed
        #       send will break the reader since those bytes won't be
        #       populated in the stream.

    async def writeFromFile(self, filepath: str, prefix: bytes = b"") -> None:
        """Write file at 'filepath' to stream with proper prefix header

        Optionally 'prefix' can be provided as bytes to include before the
        file is sent (e.g. for metadata if the stream is being processed
        by prefix bytes, etc)"""
        # Pattern from:
        # https://github.com/Tinche/aiofiles/blob/7031341b6bd8f2eacd0818dd8223dd9dc1bada7e/tests/test_os.py#L85
        # filesize could also be os.path.getsize() as sync if FS isn't a blocking problem
        assert isinstance(prefix, bytes)
        filesize: int = (await aiofiles.os.stat(filepath)).st_size
        filesizeWithPrefixHeader = filesize + len(prefix)

        header = filesizeWithPrefixHeader.to_bytes(self.headerBytes, byteorder="little")
        async with aiofiles.open(filepath, "rb") as f:  # type: ignore
            # first write total length header
            # with optional postfix determination for sub-type
            # switching on the receiver side
            async with self.writeLock:
                self.writer.write(header + prefix)
                await self.writer.drain()

                if VM_HAS_SENDFILE:
                    # now send bytes
                    filefd = f.fileno()
                    # logger.debug("Sending from file {} with size {}", filefd, filesize)
                    await self.writeFromSendfile(filefd, filesize)
                else:
                    self.writer.write(await f.read())
                    await self.writer.drain()

    # ==========================================================================
    # Native Data Type Sending via JSON Pickling
    # ==========================================================================
    def preparePickleWithHeader(self, obj: Any) -> bytes:
        """Can be used if you need to broadcast the same object as a
        header+pickle to multiple receivers without re-encoding each time.
        """
        pickledObj = self.dumps(obj)
        header = len(pickledObj).to_bytes(self.headerBytes, byteorder="little")
        return header + pickledObj

    async def writeObj(self, obj: Any) -> None:
        pickledObj = self.dumps(obj)
        psize = len(pickledObj)
        header = psize.to_bytes(self.headerBytes, byteorder="little")
        async with self.writeLock:
            if psize > 16384:
                # if big, send two
                self.writer.write(header)
                self.writer.write(pickledObj)
            else:
                # if small, concat buffer saves an independent send
                self.writer.write(header + pickledObj)

            await self.writer.drain()

    async def readObj(self):
        dataSizeAsEncodedBytes = await self.reader.readexactly(self.headerBytes)
        dataSize = int.from_bytes(dataSizeAsEncodedBytes, byteorder="little")
        dataAsBytes = await self.reader.readexactly(dataSize)
        return self.loads(dataAsBytes)


# ============================================================================
# Server Representations
# ============================================================================
@dataclass
class Server:
    cb: Optional[Callable] = None
    headerBytes: int = 4
    authKey: bytes = field(default=b"", repr=False)

    def __post_init__(self):
        assert authKey, f"Need an auth key to auth!"

    async def listen(self):
        await self.server

    def shutdown(self):
        pass

    async def newClientCB(self, reader, writer):
        """New clients are received in this method, wherein we
        wait for their auth challenge, then we run the
        actual callback the user requested with an argument
        of the reader/writer pickle connection.
        """
        cxn = PickleCxn(
            reader=reader,
            writer=writer,
            authKey=self.authKey,
            headerBytes=self.headerBytes,
        )
        await cxn.challengeReply()
        await cxn.challengeSend()
        await self.cb(cxn)


@dataclass
class ServerTCP(Server):
    host: Optional[str] = None
    port: Optional[int] = None

    def __post_init__(self):
        self.server = asyncio.start_server(
            self.newClientCB, self.host, self.port, reuse_port=True
        )


def defaultWebSocketEchoCallback(state, msg, websocket):
    asyncio.create_task(websocket.send(msg[:64]))


@dataclass
class ServerWebSocket(Server):
    host: Optional[str] = None
    port: Optional[int] = None

    server: Any = None

    def __post_init__(self):
        # need to hack the internals a bit to get the underlying server to await
        assert self.host
        assert self.port

        # We need a wrapper because websockets.serve() binds to the current
        # event loop, but we must wait until the chid process is running.
        self.server = self.becomeWebSocket

    def becomeWebSocket(self, state=None, forward=defaultWebSocketEchoCallback):
        if "clients" not in state.state:
            state.state["clients"] = {}
        else:
            # don't reassign an existing clients, just empty it
            state.state["clients"].clear()

        if not forward:
            forward = defaultWebSocketEchoCallback

        async def customOnConnectCallback(websocket, path):
            # TODO: add clean shutdown. right now we just crash the
            # server and clients see an immediate disconnect.
            # websocket servers automatically maintain a connected client
            # dict from the viewpoint of the callback.
            clients = state.state["clients"]
            clients[websocket] = None

            try:
                if asyncio.iscoroutinefunction(forward):
                    async for msg in websocket:
                        await forward(state, msg, websocket)
                else:
                    async for msg in websocket:
                        forward(state, msg, websocket)
            except websockets.ConnectionClosed:
                # ConnectionClosedError and ConnectionClosedOK are subclasses of ConnectionClosed
                # https://websockets.readthedocs.io/en/stable/_modules/websockets/exceptions.html
                return
            except Exception as e:
                # anything else too. if anything else gets this high it is bad.
                return
            finally:
                logger.trace("[websocket] Removing client:  {}", websocket)
                del clients[websocket]

        async def doWebSocket():
            return await websockets.serve(customOnConnectCallback, self.host, self.port)

        return doWebSocket


@dataclass
class ServerUnix(Server):
    path: Optional[str] = None

    server: Any = None

    def __post_init__(self):
        assert self.path is not None
        assert self.cb is not None
        self.server = asyncio.start_unix_server(self.cb, self.path)


# ============================================================================
# Client Representations
# ============================================================================
@dataclass
class Client:
    """Client objects have a .cxn we can read from and write to."""

    cxn: Optional[PickleCxn] = None
    headerBytes: int = 4
    authKey: Optional[bytes] = field(default=None, repr=False)

    def closed(self) -> bool:
        assert self.cxn
        return self.cxn.reader.at_eof()

    async def newServerCB(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """When connecting to a server, this is run before
        the offcial user callback.
        We send our client auth challenge, then run the
        actual callback the user requested with an argument
        of the reader/writer pickle connection.
        """
        assert self.authKey is not None
        self.cxn = PickleCxn(
            reader=reader,
            writer=writer,
            authKey=self.authKey,
            headerBytes=self.headerBytes,
        )

        # client sends challenge, server replies
        # then server sends challenge, client replies
        await self.cxn.challengeSend()
        await self.cxn.challengeReply()

    # simple wrappers around the inner connection methods

    def writeObj(self, *args) -> Awaitable[None]:
        return self.cxn.writeObj(*args)

    def tryWriteBytes(self, b: bytes) -> Awaitable[None]:
        return self.cxn.tryWriteBytes(b)

    def writeBytes(self, b: bytes) -> Awaitable[None]:
        return self.cxn.writeBytes(b)

    def writeFromFile(self, *args, **kwargs) -> Awaitable[None]:
        return self.cxn.writeFromFile(*args, **kwargs)

    def readBytesStreaming(self) -> AsyncIterable[Tuple[int, bytes]]:
        return self.cxn.readBytesStreaming()

    def readIntoFile(self, targetPath: Union[str, Path]) -> Awaitable[int]:
        return self.cxn.readIntoFile(targetPath)


@dataclass
class ClientTCP(Client):
    host: Optional[str] = None
    port: Optional[int] = None

    async def connect(self) -> None:
        """Connect to server."""
        assert self.host is not None
        assert self.port is not None
        # also increase default buffer limit from 64 KB to 16 MB
        reader, writer = await asyncio.open_connection(
            self.host, self.port, limit=(1 << 24)
        )

        await self.newServerCB(reader, writer)


@dataclass
class ClientUnix(Client):
    path: Optional[str] = None

    async def connect(self) -> None:
        """Connect to server."""
        assert self.path is not None
        reader, writer = await asyncio.open_unix_connection(self.path)

        await self.newServerCB(reader, writer)


@dataclass
class ClientWebSocket(Client):
    uri: Optional[str] = None

    def __post_init__(self):
        assert self.uri is not None

        def becomeWebSocketClient(p, worker):
            async def doWebSocket():
                return await self.becomeWebSocketClient(p, worker)

            return doWebSocket

        # client mode gets .connect properties (server mode gets .server)
        self.connect = becomeWebSocketClient

    async def becomeWebSocketClient(self, state, worker):
        """Connect to server."""
        # This is the "basic" way to make an auto-reconnecting websocket client.
        # Yes, it's a mess of nested exception handlers, but it's what everybody
        # else seems to do.
        while True:
            try:
                while True:
                    try:
                        # Since the worker takes full control of the connection
                        # from here and runs its own 'while True' event handler
                        # loop, we can just use a context manager here.
                        async with websockets.connect(
                            self.uri,
                            ping_interval=300,
                            ping_timeout=300,
                            close_timeout=1,
                            max_queue=1024,
                            read_limit=2**20,
                        ) as websocket:
                            # 'worker' should never return unless it wants
                            # the connection to completely disconnect without a
                            # reconnect attempt.
                            if await worker(state, websocket) == True:
                                return
                    except (
                        asyncio.TimeoutError,
                        websockets.ConnectionClosed,
                    ):
                        try:
                            # do we need a manual ping pong?
                            await asyncio.wait_for(
                                await self.websocket.ping(), timeout=3
                            )
                            # it worked!
                            continue
                        except:
                            await asyncio.sleep(0.100)
                            # break to most recent True for retry
                            break
            except socket.gaierror as err:
                logger.error("Socket error: {}", err)
                await asyncio.sleep(0.100)
                # break to top level True
                continue
            except ConnectionRefusedError:
                logger.error("Connection refused for {}", self.uri)
                # retry connection
                await asyncio.sleep(0.100)
                continue
            except KeyboardInterrupt:
                # somebody CTRL-C'd us, so go away quickly.
                sys.exit(-1)
                break
            except Exception as e:
                logger.exception("Websocket session exception: {e}", e)
                continue
