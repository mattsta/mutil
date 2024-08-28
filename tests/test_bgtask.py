import asyncio
from mutil.bgtask import BGSchedule, BGTask, BGTasks
from loguru import logger

import pytest
import pytest_asyncio


def test_create():
    ts = BGTasks("empty")


@pytest.mark.asyncio
async def test_run_one():
    ts = BGTasks("one")

    async def hello():
        logger.info("HELLO!")

    ts.create("hello test", hello())
    ts.report()
    await asyncio.sleep(0)
    ts.report()


@pytest.mark.asyncio
async def test_run_one_repeated():
    ts = BGTasks("one repeat")
    ts.final = ts.report

    async def hello():
        logger.info("HELLO PAUSE!")

    # Note: for _repeating_ runs, we pass in a function and _not_ a coroutine because
    #       the run loop must _create_ a new coroutine for each new iteration.
    ts.create("hello test pause", hello, schedule=BGSchedule(runtimes=5, pause=1))
    ts.report()
    await asyncio.sleep(0)
    ts.report()

    # wait for tasks to finish
    await asyncio.gather(*ts.tasks)
    ts.report()


@pytest.mark.asyncio
async def test_run_one_repeated_stop_early_fn():
    ts = BGTasks("one repeat")
    ts.final = ts.report

    async def hello():
        logger.info("HELLO DELETE FN!")

    # Note: for _repeating_ runs, we pass in a function and _not_ a coroutine because
    #       the run loop must _create_ a new coroutine for each new iteration.
    task = ts.create(
        "hello test pause delete fn",
        hello,
        schedule=BGSchedule(runtimes=5, pause=1, delay=10),
    )
    ts.report()
    await asyncio.sleep(0)

    # stop task BEFORE it runs to verify we are properly cleaning up coroutines which don't run
    ts.stop(task)
    ts.report()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(*ts.tasks)

    ts.report()


@pytest.mark.asyncio
async def test_run_one_repeated_stop_early_coroutine():
    ts = BGTasks("one repeat")
    ts.final = ts.report

    async def hello():
        logger.info("HELLO DELETE CORO!")

    # Note: for _repeating_ runs, we pass in a function and _not_ a coroutine because
    #       the run loop must _create_ a new coroutine for each new iteration.
    task = ts.create(
        "hello test pause delete coro",
        hello(),
        schedule=BGSchedule(runtimes=1, pause=1, delay=10),
    )
    ts.report()
    await asyncio.sleep(0)

    # stop task BEFORE it runs to verify we are properly cleaning up coroutines which don't run
    ts.stop(task)
    ts.report()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(*ts.tasks)

    ts.report()
