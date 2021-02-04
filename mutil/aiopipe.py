import os
import io
import sys
import fcntl
import asyncio

from dataclasses import dataclass
from typing import Union, List, Tuple, Any, Optional, Awaitable

import orjson

# Originally from: https://gist.github.com/mosquito/d54547f1dc8d2eb32fd8b41c4690cde3
# Heavily modified to actually work and not crash programs when used.

# Could upgrade to: https://github.com/mosquito/aio-pipe ?
# Would require adding the same modifications here for tracking sizes for complete sends and receives


# Also see:
# c.f. https://docs.python.org/3/library/asyncio-eventloop.html#working-with-pipes
# c.f. https://docs.python.org/3/library/asyncio-protocol.html#buffered-streaming-protocols
# c.f. https://docs.python.org/3/library/asyncio-eventloop.html#watching-file-descriptors


@dataclass
class AsyncPIPE:
    readFromFd: Optional[int] = None
    headerBytes: int = 4
    read_fd: Optional[int] = None
    write_fd: Optional[int] = None

    @staticmethod
    def create_pipe() -> Tuple[int, int]:
        read_fd, write_fd = os.pipe()
        for fd in (read_fd, write_fd):
            fcntl.fcntl(fd, fcntl.F_SETFL, os.O_NONBLOCK)

        return read_fd, write_fd

    def __post_init__(self) -> None:
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        if self.readFromFd:
            self.read_fd = self.readFromFd
            self.write_fd = None
        else:
            self.read_fd, self.write_fd = self.create_pipe()

        # Write futures are: (Future, [data, size-of-written-so-far])
        self._write_futures: List[Tuple[Awaitable, List[Union[bytes, int]]]] = []

        # Read futures are: (Future, size-to-read)
        self._read_futures: List[Tuple[Awaitable, int]] = []

    def setup(self, log: bool = True) -> None:
        """Note: setup must be run on client process, not on parent of
        all other child processes.

        This must be the event loop of the worker process after all."""
        self.loop = asyncio.get_event_loop()

        if not self.write_fd:
            return

        if log:
            from loguru import logger

        # On Linux we get bigger pipes, but we have to request them each time.
        # Also note: only works if the proper /proc entries are increased.
        if sys.platform == "linux":
            with open("/proc/sys/fs/pipe-max-size") as fpm:
                maxSize = int(fpm.read())

            write_fd = self.write_fd

            # x86_64-linux-gnu/bits/fcntl-linux.h
            # 203:# define F_SETPIPE_SZ 1031    /* Set pipe page size array.  */
            # 204:# define F_GETPIPE_SZ 1032    /* Set pipe page size array.  */
            origSz = fcntl.fcntl(write_fd, 1032)
            if log:
                logger.trace(
                    f"Current pipe size is: {origSz} but system max per user is {maxSize}"
                )

            trySz = maxSize
            while trySz > origSz:
                sz = trySz
                try:
                    if log:
                        logger.trace(f"Trying {trySz}...")
                    sz = fcntl.fcntl(write_fd, 1031, trySz)
                    if log:
                        logger.trace(f"Using {sz}!")
                    break
                except:
                    # failed to set, so try smaller...
                    trySz //= 2
                    continue

            if log:
                logger.info(f"New pipe size is: {sz / 1024 / 1024} MB")

    def on_read(self):
        if self._read_futures:
            f, size = self._read_futures.pop(0)
            buff = os.read(self.read_fd, size)
            try:
                f.set_result(buff)
            except asyncio.InvalidStateError:
                # when shutting down things get a bit noisy
                pass

        self.loop.remove_reader(self.read_fd)

    def on_write(self):
        # If we still have things to write, write ONE thing, then wait.
        if self._write_futures:
            f, dataBundle = self._write_futures[0]
            data, sentBytes = dataBundle
            sentBytes += os.write(self.write_fd, data[sentBytes:])

            # Pipes have a limited buffer, so they often truncate the
            # write. We need to track which data to send next.

            # If complete, signal complete.
            if sentBytes == len(data):
                del self._write_futures[0]
                f.set_result(True)
            else:
                # else, send isn't complete yet, so
                # update next write offset and cycle again
                dataBundle[1] = sentBytes
        else:
            # else, no things to write anymore, stop write polling
            self.loop.remove_writer(self.write_fd)

    def __del__(self):
        self.close()

    def close(self):
        if self.read_fd is None:
            return

        read_fd, write_fd = self.read_fd, self.write_fd
        self.read_fd, self.write_fd = None, None

        if self.loop:
            self.loop.remove_reader(read_fd)
            self.loop.remove_writer(write_fd)

        os.close(read_fd)

        if self.write_fd:
            os.close(write_fd)

    def write(self, data: bytes):
        """Append 'data' to our write queue and return and awaitable future
        so the caller will be notified when the write is complete."""
        assert self.loop

        f = self.loop.create_future()
        self._write_futures.append((f, [data, 0]))

        assert isinstance(self.write_fd, int)
        self.loop.add_writer(self.write_fd, self.on_write)
        return f

    async def writeWithHeader(self, data: bytes) -> None:
        """Append 'data' to the write buffer, but prepend the length of the
        data as an unsigned integer first in the sending stream.
        """
        allF = []
        allF.append(
            self.write(len(data).to_bytes(self.headerBytes, byteorder="little"))
        )
        allF.append(self.write(data))
        await asyncio.gather(*allF)

    async def readWithHeader(self) -> bytes:
        """Read a previously writeWithHeader() operation.

        Parses the unsigned length header and only returns to the caller when
        all of the bytes have been received.

        Returns resulting data as one contiguous byte array."""
        header: bytes = await self.read(self.headerBytes)
        totalLen: int = int.from_bytes(header, byteorder="little")
        totalRead: int = 0
        result = io.BytesIO()
        while totalRead < totalLen:
            got = await self.read(totalLen - totalRead)
            totalRead += len(got)
            result.write(got)

        # orjson doesn't like the memoryview from .getbuffer()
        # restore .getbuffer() when orjson doesn't fail with " Input must be bytes, bytearray, or str:"
        # return result.getbuffer()
        return result.getvalue()

    def read(self, size):
        """Basic read interface. Client must loop to retrieve all bytes if
        outstanding data is more than 'size' big (or if the sender or OS hasn't
        finished processing 'size' bytes to send yet)."""
        f = self.loop.create_future()
        self._read_futures.append((f, size))
        self.loop.add_reader(self.read_fd, self.on_read)
        return f

    async def writePickle(self, p):
        # Based on our testing, orjson is faster than pickle at
        # writing all objects and reading them back.
        # Pickle does have an advantage when serializing larger
        # objects (multi-MB objects), but we expect this IPC to be
        # smaller structs and objects across processes.

        # Note: we don't have to .decode() orjson here because we're fine
        # sending binary directly!
        return await self.writeWithHeader(orjson.dumps(p))

    async def writeJSONString(self, p):
        """Write an ALREADY JSON FORMATTED string into the pipe.

        Note: this JSON string will be unpacked as JSON on the other
        side so only send valid JSON things through.

        This is an optimization so we can forward JSON directly from
        a network onto a series of workers without decoding it first."""
        return await self.writeWithHeader(p)

    async def readPickle(self) -> Any:
        """ Convert data bytes (in JSON format) to object then return """
        return orjson.loads(await self.readWithHeader())

    async def readActual(self) -> bytes:
        """ Return data bytes from pipe without any conversion """
        return await self.readWithHeader()
