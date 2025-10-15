import asyncio
from typing import Any

import pytest

from mutil.dispatch import DArg, Dispatch, Op


class SleepOp(Op):
    def argmap(self) -> list[DArg]:
        return [DArg("x", convert=int)]

    async def run(self) -> Any:
        await asyncio.sleep(0.01)
        return self.x * 2


def test_async_run_is_awaited_and_value_propagated():
    async def logic():
        d = Dispatch({"s": SleepOp})
        res = await d.runop("s", "5")
        assert res == 10

    asyncio.run(logic())


class RaiseOp(Op):
    async def run(self) -> Any:
        raise RuntimeError("boom")


def test_run_exception_surfaces():
    async def logic():
        d = Dispatch({"r": RaiseOp})
        with pytest.raises(RuntimeError):
            await d.runop("r")

    asyncio.run(logic())


