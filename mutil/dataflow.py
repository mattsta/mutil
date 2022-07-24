""" A hierarchical task processing and forwarding system.

Each level of the processing can be input via WebSockets or unix pipes,
transform the input data, then forward to the next level which itself
can be any of WebSockets or unix pipes"""

from typing import Callable, Iterable, Tuple, List, Optional, Any, Union, Dict, Sequence
from collections import deque, defaultdict
from dataclasses import dataclass, field
import multiprocessing
import inspect

# from threading import Thread

from .foreverloop import ForeverLoop
from .aiopipe import AsyncPIPE
from . import aioserver

import sys
import asyncio

from loguru import logger

import setproctitle  # type: ignore
import websockets

from enum import Enum
import os

isLinux = sys.platform == "linux"

fmt = (
    "<yellow>{process.id:>5}:{process.name:<12}</yellow> "  #:{thread.name}</yellow> "
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

# Can also use callables for API logging!
#         >>> async def publish(message):
#        ...     await api.post(message)
#        ...
#        >>> logger.add(publish, serialize=True)

logger.remove()
if isLinux:
    logger.add("generation-{time}.log", level="INFO", format=fmt, enqueue=True)
else:
    logger.add(sys.stderr, level="INFO", format=fmt, enqueue=True)


# 'Listen' means dataflow will open a server for listening.
# 'Connect' means dataflow will connect to a specified server for receiving.
class NetworkMode(str, Enum):
    Listen = "Listen"
    Connect = "Connect"
    Worker = "Worker"


# WebSocket vs. multiprocessing.connection()
# https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.connection
# (could obviously add more here... http, etc)
Server = Enum("Server", "WebSocket Python PythonWebSocket")

# Method for layer forwarding to next layer
# Either hash the input (by what?) or broadcast to full width of next layer
Forward = Enum("Forward", "Hash Broadcast")


@dataclass
class NetworkCxn:
    host: Optional[str] = None
    port: Optional[int] = None
    path: Optional[str] = None
    proto: Optional[Server] = None
    mode: Optional[NetworkMode] = None
    auth: Optional[Union[str, Callable]] = None
    pipes: Optional[List[AsyncPIPE]] = None

    # filter is run by the PARENT connection to check if
    # the further connection wants data forwarded
    filter: Optional[Callable] = None

    # how to reply to clients if clients send input
    forwardFromClient: Optional[Callable] = None

    # forward is run if filter allows it
    forwardFromInternal: Optional[Callable] = None

    # optional startup / init function
    startup: Optional[Callable] = None

    # When server or client is configured, this is the coroutine we use to start it
    cxn: Optional[Callable] = None


@dataclass
class PythonCxn(NetworkCxn):
    proto: Server = Server.Python


@dataclass
class WebSocketCxn(NetworkCxn):
    proto: Server = Server.WebSocket

    # Clients have URI instead of host/port, so maybe
    # WebSocketCxn should actually be different classes for
    # clients vs. servers now.
    uri: Optional[str] = None


@dataclass
class Pipe:
    # if 'filter' exists and returns True, the element is allowed on the pipe.
    # else, the element is not sent on this pipe.
    pipe: AsyncPIPE
    filter: Optional[Callable] = None

    async def asyncSendWithFilter(self, what):
        if (not self.filter) or self.filter(what):
            await self.pipe.writePickle(what)

    def sendWithFilter(self, what):
        """Send data based on if this pipe filter a.) exists and b.) allows it.

        If pipe filter does not exist, we always allow the send.
        """

        # If filter doesn't exist, do the send.
        # If filter returns true, do the send.
        if (not self.filter) or self.filter(what):
            asyncio.create_task(self.pipe.writePickle(what))

    async def asyncSendJSONStringAlways(self, what):
        await self.pipe.writeJSONString(what)

    async def asyncSendAlways(self, what):
        """Send data without regard to any filter on the pipe."""
        await self.pipe.writePickle(what)

    def sendAlways(self, what):
        """Send data without regard to any filter on the pipe."""
        asyncio.create_task(self.pipe.writePickle(what))


@dataclass
class Runner:
    """Represent something to run in a processing layer"""

    target: Optional[Callable] = None
    #    args: Iterable
    #    kwargs: dict = None
    howMany: Optional[int] = 1

    # optionally restrict what we allow pipes ot send to us
    filter: Optional[Callable] = None

    # optional startup / init function
    startup: Optional[Callable] = None

    # Optional list of fds we inherited from a parent to read from
    readPipes: Optional[List[int]] = None

    pipesByWorker: Optional[List[List[AsyncPIPE]]] = None
    pipesByTarget: List[List[AsyncPIPE]] = field(default_factory=list)

    def setupPipes(self, inboundCount=0):
        # Generate one list of 'inboundCount' pipes for each 'howMany'
        # for this runner level.

        # this is equivalent to the consumers-per-network math
        # in Network.setupPipes()
        if self.readPipes:
            for inpipe in self.readPipes:
                self.pipesByTarget.append(
                    [Pipe(pipe=AsyncPIPE(readFromFd=inpipe), filter=self.filter)]
                )
        else:
            for idx in range(self.howMany):
                self.pipesByTarget.append(
                    [
                        Pipe(pipe=AsyncPIPE(), filter=self.filter)
                        for _ in range(inboundCount)
                    ]
                )

        self.pipesByWorker = list(zip(*[x for x in self.pipesByTarget]))

    def setup(self, inboundCount=0):
        # don't allow double init
        if self.pipesByWorker:
            return

        self.setupPipes(inboundCount)

    def __post_init__(self):
        # 0 or None means we use our pyton-detected core count
        # (which is actually the full thread count; python doesn't
        # provide a way to get the physical core count without using
        # linux internals...)
        if self.howMany == None:
            self.howMany = os.cpu_count()

        # also, if we have explict read pipes, we use one process per
        # read pipe, overriding any user provided 'howMany' value
        if self.readPipes:
            self.howMany = len(self.readPipes)


@dataclass
class Network:
    networks: Optional[Sequence[NetworkCxn]] = None
    pipesByWorker: Optional[List[List[AsyncPIPE]]] = None

    def newClientCB(self, reader, writer):
        pass

    def setupPipes(self, inboundCount=0):
        for network in self.networks:
            network.pipes = [
                Pipe(pipe=AsyncPIPE(), filter=network.filter)
                for _ in range(inboundCount)
            ]

        self.pipesByWorker = list(zip(*[network.pipes for network in self.networks]))

    def setup(self, inboundCount=0):
        if self.pipesByWorker:
            # don't run setup again if we already ran it!
            # (makes the forward-topology-setup code a little easier
            #  since we can blindly re-setup the same after-networks)
            return

        for network in self.networks:
            network.pipes = [
                Pipe(pipe=AsyncPIPE(), filter=network.filter)
                for _ in range(inboundCount)
            ]

            # giant tree of if/if/if is ugly, but it works for now.
            if network.mode == NetworkMode.Listen:
                if network.proto == Server.Python:
                    # Listening Python
                    if network.host:
                        network.cxn = aioserver.ServerTCP(
                            network.host, network.port, self.newClientCB, network.auth
                        )
                    elif network.path:
                        network.cxn = aioserver.ServerUnix(
                            path=network.path, cb=self.newClientCB, authKey=network.auth
                        )
                elif network.proto == Server.WebSocket:
                    # Listening WebSocket
                    assert network.host
                    assert network.port
                    network.cxn = aioserver.ServerWebSocket(
                        host=network.host, port=network.port
                    )
                network.start = network.cxn.server
            elif network.mode == NetworkMode.Connect:
                if network.proto == Server.Python:
                    # Connect to Python
                    if network.host:
                        network.cxn = aioserver.ClientTCP(
                            network.host, network.port, network.auth
                        )
                    elif network.path:
                        network.cxn = aioserver.ClientUnix(network.path, network.auth)
                elif network.proto == Server.WebSocket:
                    # Connect to WebSocket
                    network.cxn = aioserver.ClientWebSocket(uri=network.uri)
                network.start = network.cxn.connect
            else:
                raise Exception(f"Unexpected mode + proto? Got: {network}")

        self.setupPipes(inboundCount)


@dataclass
class NetworkRunner(Network, Runner):
    """Network runners do two things:
        - create an inbound network connection
        - run a function on each inbound item
        - send transformed data to next output layer

    e.g. a websocket inbound connection is receiving arrays
    of datapoints ([1, 2, 3, 4, 5]) but we don't want to forward
    the entire array, we want to forward individual elements.

    So the processing function will load the json into python
    objects, iterate over them, then send each one to the next
    output layer (potentially by hashing them to multiple workers
    in the next output layer)
    """

    def __post_init__(self):
        assert self.networks, f"Your NetworkRunner has no networks!"


@dataclass
class Process:
    id: int = -1
    mode: Optional[NetworkMode] = None
    server: Optional[Network] = None
    forwardFromInternal: Optional[Callable] = None
    forwardFromClient: Optional[Callable] = None
    inbound: Optional[Iterable[AsyncPIPE]] = None
    outbound: Optional[Iterable[AsyncPIPE]] = None

    # startup procedure
    # "reconnect" is just beforeConnect -> server -> afterConnect again
    beforeConnect: Optional[Callable] = None
    afterConnect: Optional[Callable] = None
    startup: Optional[Callable] = None

    # user state holder for anything user wants to remember across callbacks
    state: Dict[Any, Any] = field(default_factory=lambda: defaultdict(list))


def dataflowProcess(p: Process):
    """Wrapper for running user requested function.

    This wraper auto-initializes all inbound and outbound pipes
    for this process so the underlying function doesn't need to
    worry about any setup. All pipes will be good to go.

    All arguments are optional because this can form either:
        - an inbound consumer
            - connects to external service then forwards to outbound
              using 'forward' function
        - a processing layer
            - reads from 'inbound', processes using 'forward' function,
              and writes to 'outbound'
        - a reply layer
            - reads from 'inbound' and writes to server connections
    """

    setproctitle.setproctitle(f"{sys.argv[0]} [{p.mode}:{p.id}]")

    if isLinux:
        import prctl  # type: ignore
        import signal

        # tell the kernel to kill this process when the parent exits
        prctl.set_pdeathsig(signal.SIGKILL)

    async def runProcessing():
        # Setup all AsyncPIPEs passed as kwargs
        # (either 'inbound' or 'outbound' (or both at once)

        # For the first layer of consumers and the last layer of repliers,
        # 'inbound' and 'outbound' will be just lists of AsyncPIPE.
        # For the middle layer of processors, 'inbound' and 'outbound' are
        # lists of tuples.
        # logger.info(f"Inbound: {p.inbound}")
        # logger.info(f"Outbound: {p.outbound}")
        # Double convert lists here because sometimes inputs are tuple vs. list
        for pipe in list(p.inbound) + list(p.outbound):
            pipe.pipe.setup()

        assert not (
            p.server and (p.inbound and p.outbound)
        ), "Server layers can't have inbound and outbound pipes at the same time!"

        # If this is a server level, we run the server directly and the
        # server spawns new async tasks for each client based on a
        # new client callback.
        if p.server:
            # Run a network connection (client or server) with (optional) processing callback
            # logger.info("Running server: {} :: {}", p, p.forwardFromClient)
            runServer = p.server(p, p.forwardFromClient)
            asyncio.create_task(runServer())

        # now configure if this is a processing level or a forwarding level.
        # (if this is the first consume level, 'inbound' will not be populated,
        # so the rest of this function is a noop)

        # run on-start setup if needed / if exists
        if p.startup:
            if asyncio.iscoroutinefunction(p.startup):
                await p.startup(p)
            else:
                p.startup(p)

        if p.inbound:
            assert p.forwardFromInternal, "No forward for level?"

            fl = ForeverLoop()
            # add all inbound pipes to our processing loop

            # if reader function has the data argument as type binary, return
            # the data instead of converting it first!
            aspec = inspect.getfullargspec(p.forwardFromInternal)
            annos = aspec.annotations
            secondArgName = aspec.args[1]
            typeOfSecond = annos.get(secondArgName)
            if typeOfSecond == bytes:
                # requested binary input so return the data bytes directly
                for i in p.inbound:
                    fl.addRunner(i.pipe.readActual)
            else:
                # else, decode python to objects!
                for i in p.inbound:
                    fl.addRunner(i.pipe.readPickle)

            # now dispatch input from inbound pipes to callback, forever.
            # (we factor out into two functions so we aren't having to
            #  evaluate the "iscoroutinefunction()" condition on each
            #  new inbound received data)
            if asyncio.iscoroutinefunction(p.forwardFromInternal):
                async for got in fl.runForever():
                    # Run with internal server callback (async)
                    got = await got
                    try:
                        await p.forwardFromInternal(p, got)
                    except Exception as e:
                        logger.exception(
                            f"Error while iterating async worker? {p}, {got}"
                        )
            else:
                async for got in fl.runForever():
                    # Run with internal server callback (sync)
                    got = await got
                    try:
                        p.forwardFromInternal(p, got)
                    except Exception as e:
                        logger.exception(f"Error while iterating worker? {p}, {got}")

    async def startProcessing():
        # This is just a simple way of catching all exceptions in the forked
        # process without wrapping the entire processing function in an
        # extra level of 'try/except' indentation.
        try:
            with logger.contextualize(task_id=p.id):
                logger.info("Starting...")
                await runProcessing()
        except KeyboardInterrupt:
            # ignore control-c
            pass
        except asyncio.CancelledError:
            # also control-c
            pass
        except:
            logger.exception("huh what?")

    loop = asyncio.get_event_loop()
    loop.create_task(startProcessing())
    loop.run_forever()
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


@dataclass
class Dataflow:
    """Dataflow provides input arguments to producer function then
    sends results to a forwarding process.

    Each of 'producer' and 'processor' and 'forwarder' will be run in event loops.

    Each of P/P/F can be run multiple times.
    A typical pattern will be one producer (receiving outside data) sending data
    to multiple processors (either RR or hashed by a data key), then each processor
    sends results to a single forwarder. Forwarder will listen on pipes created for
    each of the processors to send streams of data through.

    [data stream] --> [producer] to N --> {processors} to 1 --> [forwarder]
    """

    name: str
    topology: Sequence[Union[Network, Runner]]

    #    inbound: Network

    # We may have multiple forwarders (e.g. python, websocket, http) and each
    # forwarder can have a filter defining what data they want to accept
    # before forwarding.
    # Also optional to have no forwarder if this is a consume+process only node.
    #    outbound: Optional[List[Network]] = None

    # data processing is optional.
    # it's valid to just have a producer -> forwarder relationship
    #    processing: Optional[Runner] = None

    processMap: list = field(default_factory=list)

    def setupTopology(self):
        # for each level of the topology, configure such that:
        #   - the OUTPUT of the CURRENT LEVEL is:
        #       - the CURRENT LEVEL WORKERS * NEXT LEVEL WORKERS
        #       - (held by the next level)
        #   - the INPUT to the NEXT LEVEL is:
        #       - the NEXT LEVEL WORKERS (as specified above)
        #       - (held by the next level, since it has its own input,
        #          which is the output from the previous level)
        #   - etc
        #   - the FIRST LEVEL has NO INPUT PIPES
        #   - the LAST LEVEL has NO OUTPUT PIPES
        for levelIdx, level in enumerate(self.topology):
            if isinstance(level, Network):
                # networks have one runner per endpoint
                currentSize = len(level.networks)
            else:
                # processing layers can be as wide as needed
                assert isinstance(level, Runner)
                currentSize = level.howMany

            # if we have a next level, use it
            nextLevel = None
            if levelIdx + 1 < len(self.topology):
                nextLevel = self.topology[levelIdx + 1]

            if levelIdx == 0:
                # first level has no input pipes
                level.setup()

            if nextLevel:
                # the OUTBOUND pipes for the CURRENT level are
                # defined as: NEXT LEVEL COUNT * CURRENT LEVEL COUNT
                # the .setup() function knows the size of its own level,
                # so we just give it the current level so it creates
                # the proper number of intermediate pipes.
                nextLevel.setup(currentSize)

            if isinstance(level, NetworkRunner):
                # Launch 'len(level.networks)' numbers of processes for
                # the networks inside this level.
                for widthIdx, network in enumerate(level.networks):
                    nextPipes = []

                    if nextLevel:
                        nextPipes = nextLevel.pipesByWorker[widthIdx]

                    logger.info(
                        "[{} :: {}] Next pipes for network: {}",
                        levelIdx,
                        widthIdx,
                        nextPipes,
                    )
                    self.processMap.append(
                        Process(
                            id=f"{levelIdx}.{widthIdx}",
                            mode=network.mode,
                            server=network.start,
                            inbound=network.pipes,  # we are our own inbound
                            outbound=nextPipes,
                            forwardFromInternal=network.forwardFromInternal,
                            forwardFromClient=network.forwardFromClient,
                            startup=network.startup,
                        )
                    )
            elif isinstance(level, Runner):
                # Launch 'level.howMany' numbers of processes for
                # this runner/processor level.
                logger.info(f"Creating {level.howMany} workers!")
                for widthIdx in range(level.howMany):
                    # inbound is all the pipes for our worker index
                    i = level.pipesByTarget[widthIdx]

                    o = []
                    if nextLevel:
                        o = nextLevel.pipesByWorker[widthIdx]

                    logger.info(
                        f"[{levelIdx}.{widthIdx}] Populating inbound outbound: {i} — {o}"
                    )
                    self.processMap.append(
                        Process(
                            id=f"{levelIdx}.{widthIdx}",
                            mode=NetworkMode.Worker,
                            forwardFromInternal=level.target,
                            inbound=i,
                            outbound=o,
                            startup=level.startup,
                        )
                    )

    def __post_init__(self):
        """Given constructor input, build full process and layer mappings."""

        try:
            self.setupTopology()
        except:
            logger.exception("Stop breaking things!")

        # now we're ready to run

    def start(self):
        launched = set()

        # reverse process map so we start the lower levels before
        # the higher levels (so when the higher levels are activated,
        # they will be able to send data to the already-activated
        # lower levels immediately)
        self.processMap.reverse()
        try:
            for arg in self.processMap:
                logger.info(f"Launching with: {arg}")
                # Note: daemon flag isn't daemon, but just means
                #       cleanly shutdown child processes on exit:
                # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process.daemon
                p = multiprocessing.Process(
                    target=dataflowProcess,
                    args=(arg,),
                    daemon=True,
                    name=f"{arg.mode}:{arg.id}",
                )
                p.start()
                logger.info(f"Launched {p}")
                launched.add(p)
        except:
            logger.exception(f"Badness? {self.processMap}")

        # Wait for everything to complete...
        while True:
            for l in launched:
                try:
                    l.join() # was join(1) why?
                    if l.exitcode:
                        logger.info("Exited {}, shutting down program", l)
                        sys.exit(-1)
                except KeyboardInterrupt:
                    sys.exit(-3)
                except SystemExit:
                    # a child process exited and we caught it and we are now exiting
                    sys.exit(-2)
                except:
                    logger.exception("Process error?")
