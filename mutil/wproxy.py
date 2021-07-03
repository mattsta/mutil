import websockets
import orjson
import asyncio

# https://docs.python.org/3/library/collections.abc.html
import collections.abc

from collections import defaultdict
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


def tplatKeys(rows: list):
    return {row["sym"]: idx for idx, row in enumerate(rows)}


@dataclass
class Client:
    """Represents the interest of a client wanting upstream messages,
    potentially filtering by key."""

    ws: websockets.serve
    keys: set[str] = field(default_factory=set)

    # skip routing and just send everything
    all: bool = False

    def wants(self, routerKeys) -> List[int]:
        """Returns list of index positions from routerKeys to send to
        client based on expressed interest."""

        # Simple:
        # 'routerKeys' is a dict of SYMBOL -> List Position Index
        # our 'keys' is a set of SYMBOL
        # so return the values from routerKeys where the key matches
        # an intersection with our 'keys'
        return [routerKeys[x] for x in (self.keys & routerKeys.keys())]


def subFormatPasses(data):
    """Verify a command has proper formatting (is a list) or exists at all."""
    if not isinstance(data, list):
        ws.send(orjson.dumps(dict(error="Subscribe items must be a list")))
        return False

    if not data:
        return False

    return True


@dataclass
class Server:
    uri: str  # websocket URI if no custom connect implementation

    async def connect(self):
        """Default connect method to connect to connect URI.

        Override in subclasses if different connection behavior is required."""
        logger.info("Connecting to upstream: {}", self.uri)
        self.upstream = await websockets.connect(self.uri, **DEFAULT_WS_PARAMS)

    async def updateSubscribe(self, currentKeys, newKeys, removedKeys):
        """Add your custom subscribe and unsubscribe logic here.

        Parameters are provided as:
        - current set of all subscribed keys on this proxy
        - set of new keys subscribed for this call (included in 'currentKeys')
        - set of removed keys to unsubscribe (previously in 'currentKeys' but now removed)"""
        pass

    def recv(self):
        return self.upstream.recv()

    def close(self):
        return self.upstream.close()


@dataclass
class WProxy:
    listen: Tuple[str, int]
    server: Server  # upstream server connection

    # After upstream connect, run method if user needs to provide
    # authentication / setup / accounting / etc
    onConnect: Optional[Callable] = None

    # Returns a list of (key, idx) tuples from the upstream received object to use for
    # routing upstream objects to clients subscribed to specific keys.
    # Note: if client is subscribed to '*', then no key filtering is
    #       applied to the client.
    routerKeys: Optional[Callable] = None

    # Function to take the previous subscribe list and broken out sets
    # of new symbols to subscribe and unsubscribe then perform the required
    # subscribe/unsubscribe action on the upstream.
    # It receives full prev and breakout sub/unsub because some services
    # only accept "full subscribe list" upates instead of allowing per-key
    # unsub removals or per-key sub additions.
    updateSubscribe: Optional[Callable] = None

    # TODO: create a custom 'set' subset where .add and .remove are subclassed
    # to queue additions and removals if set is currently being iterated over.
    clients: set = field(default_factory=set)
    sending: bool = False  # whether 'clients' is currently being iterated

    # Map of KEY -> {CLIENTS} for upstream to downstream routing
    subscribed: dict = field(default_factory=lambda: defaultdict(set))

    # Maps of KEY -> {CLIENTS} for updating 'subscribed' after an inbound client request.
    pendingSub: dict = field(default_factory=lambda: defaultdict(set))
    pendingUnsub: dict = field(default_factory=lambda: defaultdict(set))

    async def setup(self):
        """Open listen connection and connect to upstream."""
        logger.info("Listening on [{}]:{}", self.listen[0], self.listen[1])
        self.downstream = await websockets.serve(
            self.subscribe, self.listen[0], self.listen[1]
        )

        await self.connectUpstream()

    async def connectUpstream(self):
        while True:
            try:
                logger.info("Connecting to upstream...")
                await self.server.connect()
                break
            except:
                logger.exception("Failed to connect?")
                await asyncio.sleep(3)
                continue

        if self.onConnect:
            await self.onConnect()

        # if this is a reconnect, we need to re-subscribe too...
        await self.upstreamResubscribe()

    async def upstreamResubscribe(self, prev: Optional[dict[str, str]] = None):
        if self.updateSubscribe:
            currentkeys = self.subscribed.keys()

            if prev:
                prevkeys = prev.keys()

                newSubscribe: collections.abc.Set = currentkeys - prevkeys
                newUnsub = prevkeys - currentkeys
            else:
                newSubscribe = currentkeys
                newUnsub = set()

            await self.server.updateSubscribe(currentkeys, newSubscribe, newUnsub)

    async def updateSubscriptions(self):
        """Process pending subscribe and unsub requests for keys from clients.

        Note: we always process the subscriptions then the unsubscribes. The order
        given by the client is irrelevant to our processing, so if the client sends
        one data request of [{sub: [a, b, c], unsub: [a, b, c], sub: [a, b, c]}] then
        the keys are still _all_ unsubscribed (we try to mitigate this by running the
        add/remove conflict checks in the sub/unsub statement processing though)."""

        # Need to save original subscription state so we can diff it to
        # update the global upstream subscription state.
        originalSubs = self.subscribed.copy()

        hardAdd = set()
        for k, cli in self.pendingSub.items():
            if k not in self.subscribed:
                hardAdd.add(k)

            self.subscribed[k].add(cli)

        self.pendingSub.clear()

        # next do the unsubscribing
        hardRemove = set()
        for k, cli in self.pendingUnsub.items():
            self.subscribed[k].remove(cli)

            # if this was the last client, do a hard upstream unsubscribe
            if not self.subscribed[k]:
                del self.subscribed[k]
                hardRemove.add(k)

        self.pendingUnsub.clear()

        # next do the subscribing
        # we do the removals via collection because some services
        # require a "request the full list of everything you want each time"
        # so individual symbols can't be unsubscribed, we have to just re-submit
        # our entire population with the unsubs removed.
        if hardAdd or hardRemove:
            await self.upstreamResubscribe(originalSubs)

    async def subscribe(self, ws, path):
        client = Client(ws)

        async def addClient():
            """We can only modify the 'clients' set if we are not
            currently iterating over it."""
            if not self.sending:
                self.clients.add(client)
            else:
                asyncio.create_task(addClient())

        async def removeClient():
            if not self.sending:
                self.clients.remove(client)
            else:
                asyncio.create_task(removeClient())

        try:
            await addClient()

            # TODO: add optional pub/sub infrastructure.
            # Right now this is just a subscription repeater

            async for message in ws:
                # for now just echo anything back since we aren't doing
                # per-request upstream pub/sub accounting/tracking.
                try:
                    req = orjson.loads(message)
                except:
                    await ws.send("")
                    continue

                for cmd, data in req.items():
                    if cmd == "subscribe:all":
                        client.all = True
                    elif cmd == "unsubscribe:all":
                        client.all = False
                    elif cmd == "subscribe":
                        if not subFormatPasses(data):
                            continue

                        for key in data:
                            client.keys.add(key)

                            # detect conflicts
                            # if an unsub is pending and we get a subscribe,
                            # cancel the unsub.
                            pu = self.pendingUnsub.get(key)
                            if pu and client in pu:
                                pu.remove(client)

                            self.pendingSub[key].add(client)
                    elif cmd == "unsubscribe":
                        if not subFormatPasses(data):
                            continue

                        for key in data:
                            client.keys.remove(key)

                            # detect conflicts
                            # if a sub is pending and we get an unsub,
                            # cancel the sub.
                            ps = self.pendingSub.get(key)
                            if ps and client in ps:
                                ps.remove(client)

                            self.pendingUnsub[key].add(client)

                if self.pendingSub or self.pendingUnsub:
                    await self.updateSubscriptions()

                await ws.send(message[:200])
        except (
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.ConnectionClosedOK,
        ):
            # don't care
            pass
        finally:
            await removeClient()

    async def proxy(self):
        """Run the proxy accepting data from the upstream and
        the clients then forwarding data downstream."""
        while True:
            try:
                opened = None
                try:
                    src = await self.server.recv()
                except asyncio.CancelledError:
                    # somebody requested a clean shutdown
                    return
                except:
                    # any other error (like connection went away)
                    try:
                        await self.server.close()
                    except:
                        pass

                    # got a receive error, so reconnect
                    await self.connectUpstream()

                    # loop again for next message...
                    continue

                self.sending = True
                for client in self.clients:
                    try:
                        if client.all:
                            # client requested no filtering, so send
                            # entire feed from upstream
                            await client.send(src)
                        else:
                            # else, client needs to be filtered by
                            # subscribed key(s)
                            if not opened:
                                opened = orjson.loads(src)

                            # collect all keys from the upstream message
                            upstreamKeys = self.routerKeys(opened)

                            # check if the client is subscribed to any of
                            # these keys (in routerKeys() return format).
                            # Returns the routerKeys() format for each
                            # symbol subscribed by the client.
                            for sendIdx in client.wants(upstreamKeys):
                                await client.send(opened[sendIdx])
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
        try:
            wp = WProxy(
                listen=(listen_host, listen_port), server=Server(upstream_websocket)
            )

            # Listen for clients and connect to upstream
            await wp.setup()

            # Forward output from upstream to all clients
            await wp.proxy()
        except:
            logger.exception("Failure?")

    try:
        asyncio.run(
            asyncio.wait([doit(), cleanQuit()], return_when=asyncio.FIRST_COMPLETED)
        )
    except:
        logger.exception("Error?")


if __name__ == "__main__":
    run()
