"""A wrapper around asyncio starting to cleanly
wait for and shutdown tasks when a full exit is
signaled."""

try:
    import uvloop
except:
    # need to provide an empty value for uvloop because we
    # have 'uvloop' as a default argument later in the file.
    uvloop = None  # type: ignore

import asyncio
import collections

import sys


def shutdown_exception_handler(loop, context):
    """When Ctrl-C happens, don't log cancel errors.
    (because they aren't errors, we *do* want everything to cancel now)
    """
    if "exception" not in context or not isinstance(
        context["exception"], asyncio.CancelledError
    ):
        loop.default_exception_handler(context)


def safeLoop(main, customloop=uvloop):
    """Simple well-behaved event loop scaffolding.

    Provide either a coroutine or an iterable of coroutines to launch."""

    from inspect import getmembers

    # We need uvloop to be configurable because it breaks when using with
    # prompt_toolkit sometimes. shrug.
    if customloop:
        customloop.install()

    loop = asyncio.get_event_loop()

    # Python < 3.7 compat shim
    # python before 3.7 doesn't support asyncio.create_task(), so we want to
    # mock 'create_task' back into 'asyncio' if it doesn't exist.
    if "create_task" not in dict(getmembers(asyncio)):
        asyncio.create_task = lambda x: loop.create_task(x)  # type: ignore

    # if 'main' is a container of something, run everything inside
    if isinstance(main, collections.abc.Iterable):
        for t in main:
            loop.create_task(t)
    else:
        # else, just one coroutine to turn into a task and schedule
        loop.create_task(main)

    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        # Somebody Ctrl-C'd while event loop was waiting, so we want to exit
        shutdown()
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        print("Done!")


def shutdown():
    loop = asyncio.get_event_loop()
    # These shutdown solutions adapted from:
    # https://stackoverflow.com/a/42097478
    print("Caught exit request. Canceling tasks...")
    loop.set_exception_handler(shutdown_exception_handler)

    # Handle shutdown gracefully by waiting for all tasks to be cancelled
    tasks = asyncio.gather(
        *asyncio.all_tasks(loop=loop), loop=loop, return_exceptions=True
    )
    tasks.add_done_callback(lambda t: loop.stop())
    tasks.cancel()

    # Keep the event loop running until it is either destroyed or all
    # tasks have really terminated
    while not tasks.done() and not loop.is_closed():
        print("Not done...")
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            # be quiet and immediate exit if doing a double Ctrl-C
            sys.exit(1)
