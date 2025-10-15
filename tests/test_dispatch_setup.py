from typing import Any

import pytest

from mutil.dispatch import DArg, Dispatch, Op


class SetupOp(Op):
    def argmap(self) -> list[DArg]:
        return [
            DArg("a"),
            DArg("b", default=2),
            DArg("*rest"),
        ]

    async def run(self) -> Any:
        return self.a, self.b, self.rest


@pytest.mark.asyncio
async def test_exact_arity_sets_attributes():
    d = Dispatch({"s": SetupOp})
    result = await d.runop("s", "1 3 x y")
    assert result == ("1", "3", ["x", "y"])  # positional b overrides default


@pytest.mark.asyncio
async def test_missing_trailing_with_default_is_used():
    d = Dispatch({"s": SetupOp})
    result = await d.runop("s", "1")
    assert result == ("1", 2, [])


@pytest.mark.asyncio
async def test_rest_collects_extras():
    d = Dispatch({"s": SetupOp})
    result = await d.runop("s", "1 5 extra1 extra2")
    assert result == (
        "1",
        "5",
        ["extra1", "extra2"],
    )  # override default, rest collected


def test_state_is_passed_through():
    class StateOp(Op):
        def argmap(self) -> list[DArg]:
            return [DArg("x")]

        async def run(self) -> Any:
            return self.state, self.x

    import asyncio

    async def logic():
        d = Dispatch({"st": StateOp})
        result = await d.runop("st", "val", state={"k": 1})
        assert result == ({"k": 1}, "val")

    asyncio.run(logic())
