import asyncio
from typing import Any

from mutil.dispatch import DArg, Dispatch, Op


class DefaultConvertOp(Op):
    def argmap(self) -> list[DArg]:
        return [
            DArg("x", convert=int, verify=lambda v: v >= 0, default=0),
            DArg("*rest"),
        ]

    async def run(self) -> Any:
        return self.x, self.rest


def test_default_is_converted_and_verified():
    async def logic():
        d = Dispatch({"dc": DefaultConvertOp})
        # Missing x -> default 0, convert to int, verify >= 0
        res = await d.runop("dc", None)
        assert res == (0, [])

    asyncio.run(logic())


def test_provided_value_is_converted_and_verified():
    async def logic():
        d = Dispatch({"dc": DefaultConvertOp})
        res = await d.runop("dc", "5")
        assert res == (5, [])

    asyncio.run(logic())


