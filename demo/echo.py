#!/usr/bin/env python3

import asyncio

import orjson
from loguru import logger

from mutil import dataflow


async def inboundForward(p, blob, client):
    """Inbound forwarding gets the element from the server (depends
    on the server spec—can be bytes from websockets or an already converted
    item from a python peer server) and the FULL outbound array so it can
    make a decision about which next level pipe to send to."""

    outbound = p.outbound
    # Given our input of a JSON array, forward each element
    # to the next level by hash.
    try:
        # dicts are '.as_dict()'
        # arrays are '.as_list()'
        # if we are just iterating, we don't need to create
        # them all at once up front, but we can anyway.
        fromJSON = orjson.loads(blob)
    except:
        # avoid processing any json decode error
        asyncio.create_task(
            client.send(
                orjson.dumps(dict(error="Only JSON lists are accepted!")).decode()
            )
        )
        logger.error(f"JSON decode failed with input: {blob}")
        return

    logger.info(f"Decoded from JSON: {fromJSON}")

    # pre-caching length is faster than running len() each time
    outs = len(outbound)
    for item in fromJSON:
        if True:
            # send by hash, then only if filter matches
            N = hash(item) % outs
            logger.info(f"Sending object to worker {N}: {item}")
            out = outbound[N]
            await out.asyncSendWithFilter(item)
        else:
            # send to all workers if filter matches
            for out in outbound:
                out.sendWithFilter(item)


def processingMiddle(p, item):
    out = p.outbound
    modified = item + 1
    logger.info(f"Forwarding middle item to next middle: {modified}")
    out[hash(modified) % len(out)].sendWithFilter(modified)


def processingLast(p, item):
    """Processing forwarding gets the element from the server pipe
    already converted to a native python object.

    The final forwarder layer listens to outbound pipes for ALL workers."""
    modified = item + 1
    logger.info(f"Forwarding item to replier: {modified}")
    out = p.outbound
    for o in out:
        o.sendWithFilter(modified)


def processingReply(p, obj):
    # Send inbound object to all clients
    logger.info(f"Sending obj to clients: {obj}")
    clients = p.state["clients"]
    for client in clients:
        asyncio.create_task(client.send(orjson.dumps(obj).decode()))


def evenSetup(server):
    # Batch replies to be delivered every 250 ms
    def sendBuffers():
        if server.outBuffer:
            # Bundle encoding delivery once for all clients
            outBuffer = orjson.dumps(server.outBuffer).decode()

            # Schedule sending of recent buffer to all clients
            for client in server.clients:
                asyncio.create_task(client.send(outBuffer))

            # reset buffer for next accumulation...
            server.outBuffer.clear()

        # schedule again 250 ms in the future
        asyncio.get_event_loop().call_later(0.250, sendBuffers)

    # just run it once now so it schedules itself again
    sendBuffers()


def yes2(x):
    return x % 2 == 0


def no2(x):
    return not yes2(x)


if __name__ == "__main__":
    # python3.8 changed this behavior on macos from fork to spawn, but
    # spawn doesn't inherit pipes, so we must 'force' it back.
    import multiprocessing as mp

    mp.set_start_method("fork", force=True)

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
            filter=no2,
        ),
        dataflow.WebSocketCxn(
            host="127.0.0.1",
            port=2223,
            mode=dataflow.NetworkMode.Listen,
            forwardFromInternal=processingReply,
            filter=yes2,
            #            startup=evenSetup,
        ),
    ]

    # GUIDELINE:
    #   - ORJSON FASTEST+SMALLEST dump of ANY VALUE
    #   - ORJSON FASTEST for loading SMALL ARRAYS/OBJECST (like individual trades)
    #   - SIMDJSON FASTEST for parsing LARGE INPUTS (like the inbound aggregate trade feed with long arrays)

    # Pickle is still the smallest format for LARGE OBJECT serialization
    # For small serialization, orjson wins (simdjson adds extra spaces)

    # Goal:
    #   - connect to websocket on 2221 to send
    #   - connecto to websocket on 2222 to receive
    #   - send array of numbers [1, 2, 3, 4, 5]
    #   - worker(s) doubles all vaules: [2, 4, 6, 8, 10]
    #   - workers send reply to replier
    #   - replier sends results to all connections
    topology = [
        dataflow.NetworkRunner(networks=Inbound),
        #       dataflow.Runner(target=processingMiddle, howMany=None),
        #       dataflow.Runner(target=processingMiddle, howMany=None),
        dataflow.Runner(target=processingLast, howMany=6),
        dataflow.NetworkRunner(networks=Outbound),
    ]

    dflow = dataflow.Dataflow(
        name="echo server!",
        topology=topology,
    )

    dflow.start()
