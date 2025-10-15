import asyncio
from typing import Any

import pytest

from mutil.dispatch import Dispatch, Op


class AOp(Op):
    async def run(self) -> Any:
        return "A"


class ABOp(Op):
    async def run(self) -> Any:
        return "AB"


def test_exact_and_prefix_resolution():
    async def logic():
        d = Dispatch({"alpha": AOp, "alpine": ABOp})
        # exact
        assert await d.runop("alpha") == "A"
        # unambiguous prefix: "alpi" maps to "alpine" only
        assert await d.runop("alpi") == "AB"

    asyncio.run(logic())


def test_namespaced_resolution_and_grouping():
    async def logic():
        d = Dispatch({"grp": {"inner": AOp, "inside": ABOp}})
        # full name works
        assert await d.runop("inner") == "A"
        # prefix
        assert await d.runop("insi") == "AB"

        # ensure fullOps contains inner names
        assert "inner" in d.fullOps and "inside" in d.fullOps

    asyncio.run(logic())


def test_operation_without_run_raises():
    class NoRun(Op):
        pass

    async def logic():
        d = Dispatch({"norun": NoRun})
        with pytest.raises(NotImplementedError):
            await d.runop("nor")

    asyncio.run(logic())
