import websockets
import orjson
import asyncio

from dataclasses import dataclass, field
from typing import *

from loguru import logger

# By default the websocket client will trigger exceptions if it
# doesn't get a ping every 5 seconds which is too restrictive
# for some use cases. Just give everything easier bounds.
DEFAULT_WS_PARAMS = dict(
    ping_interval=300,
    ping_timeout=300,
    close_timeout=1,
    max_queue=256,
    read_limit=2 ** 20,
)


@dataclass
class WProxy:
    listen: Tuple[str, int]
    connect: str  # websocket URI
    routerKey: Optional[Callable] = None

    # TODO: create a custom 'set' subset where .add and .remove are subclassed
    # to queue additions and removals if set is currently being iterated over.
    clients: set = field(default_factory=set)
    sending: bool = False  # whether 'clients' is currently being iterated

    async def setup(self):
        """ Open listen connection and connect to upstream. """
        logger.info("Listening on [{}]:{}", self.listen[0], self.listen[1])
        self.downstream = await websockets.serve(
            self.subscribe, self.listen[0], self.listen[1]
        )

        logger.info("Connecting to upstream: {}", self.connect)
        self.upstream = await websockets.connect(self.connect, **DEFAULT_WS_PARAMS)

    async def subscribe(self, ws, path):
        try:

            async def addClient():
                """We can only modify the 'clients' set if we are not
                currently iterating over it."""
                if not self.sending:
                    self.clients.add(ws)
                else:
                    asyncio.create_task(addClient())

            await addClient()

            # TODO: add optional pub/sub infrastructure.
            # Right now this is just a subscription repeater

            async for message in ws:
                # for now just echo anything back since we aren't doing
                # per-request upstream pub/sub accounting/tracking.
                await ws.send(message[:200])
        except (
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.ConnectionClosedOK,
        ):
            # don't care
            pass
        finally:

            async def removeClient():
                if not self.sending:
                    self.clients.remove(ws)
                else:
                    asyncio.create_task(removeClient())

            await removeClient()

    async def proxy(self):
        """Run the proxy accepting data from the upstream and
        the clients then forwarding data downstream."""
        while True:
            try:
                src = await self.upstream.recv()

                self.sending = True
                for client in self.clients:
                    try:
                        await client.send(src)
                    except:
                        # client likely gone, will be cleaned up later
                        pass

                self.sending = False
            finally:
                pass


import click


@click.command()
@click.argument("listen_host", default="127.0.0.1")
@click.argument("listen_port", default=6222)
@click.argument("upstream_websocket", default="ws://127.0.0.1:4442")
def run(listen_host, listen_port, upstream_websocket):
    """Simple helper to run a proxy interactively.

    To run in a program, use WProxyy(listen_host, listen_port, websocket)."""
    import signal

    async def cleanQuit():
        """Watch for signals then raise a future result when we see them.

        Note: *must* be run *inside* asyncio.run() because asyncio.run()
              *creates* a new event loop each time. We can't run .get_event_loop()
              outside of .run() because it is a different loop."""
        loop = asyncio.get_event_loop()
        stop = loop.create_future()
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        await stop
        logger.warning("Exiting due to user signal...")

    async def doit():
        wp = WProxy(listen=(listen_host, listen_port), connect=upstream_websocket)

        # Listen for clients and connect to upstream
        await wp.setup()

        # Forward output from upstream to all clients
        await wp.proxy()

    try:
        asyncio.run(
            asyncio.wait([doit(), cleanQuit()], return_when=asyncio.FIRST_COMPLETED)
        )
    except:
        logger.exception("Error?")


if __name__ == "__main__":
    run()
