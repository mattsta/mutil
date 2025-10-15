import asyncio
from typing import Any

from mutil.dispatch import DArg, Dispatch, Op


class RestConvertOp(Op):
    def argmap(self) -> list[DArg]:
        # Model per-item conversion by handling conversion inside run
        return [DArg("*nums")]

    async def run(self) -> Any:
        # Convert inside run to simulate per-item conversion requirement
        try:
            nums = [int(x) for x in self.nums]
        except Exception:
            raise ValueError("bad rest item")
        return nums


def test_rest_empty_ok():
    async def logic():
        d = Dispatch({"r": RestConvertOp})
        res = await d.runop("r", None)
        assert res == []

    asyncio.run(logic())


def test_rest_all_valid_items():
    async def logic():
        d = Dispatch({"r": RestConvertOp})
        res = await d.runop("r", "1 2 3")
        assert res == [1, 2, 3]

    asyncio.run(logic())


def test_rest_invalid_item_raises():
    async def logic():
        d = Dispatch({"r": RestConvertOp})
        try:
            await d.runop("r", "1 two 3")
            assert False, "Expected ValueError"
        except ValueError:
            pass

    asyncio.run(logic())


