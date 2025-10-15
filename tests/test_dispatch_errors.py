import asyncio
from io import StringIO
from typing import Any

from loguru import logger

from mutil.dispatch import DArg, Dispatch, Op


class ErrOp(Op):
    def argmap(self) -> list[DArg]:
        return [DArg("a"), DArg("b")]

    async def run(self) -> Any:
        return None


class ValidateOp(Op):
    def argmap(self) -> list[DArg]:
        return [
            DArg("x", convert=int, verify=lambda v: v > 0, errmsg="must be > 0"),
            DArg("*rest"),
        ]

    async def run(self) -> Any:
        return None


def test_missing_required_args_logs_error():
    async def logic():
        d = Dispatch({"e": ErrOp})
        buf = StringIO()
        sink_id = logger.add(buf, level="ERROR")
        try:
            res = await d.runop("e", "only_one")
            assert res is None
        finally:
            logger.remove(sink_id)
        return buf.getvalue()

    text = asyncio.run(logic())
    assert "Not enough arguments provided" in text


def test_validation_failure_logs_details():
    async def logic():
        d = Dispatch({"v": ValidateOp})
        buf = StringIO()
        sink_id = logger.add(buf, level="ERROR")
        try:
            res = await d.runop("v", "-1")
            assert res is None
        finally:
            logger.remove(sink_id)
        return buf.getvalue()

    text = asyncio.run(logic())
    assert "Argument validation failed" in text


def test_unknown_command_prints_completion_or_all(capfd):
    async def logic():
        d = Dispatch({"alpha": ErrOp, "alpine": ErrOp})
        await d.runop("al")

    asyncio.run(logic())
    out, err = capfd.readouterr()
    # The implementation prints completions to stdout in this path
    assert "Completion choices:" in out
    assert "alpha" in out and "alpine" in out
