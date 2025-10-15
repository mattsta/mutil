import asyncio
from io import StringIO
from typing import Any

from loguru import logger

from mutil.dispatch import DArg, Dispatch, Op


class HelpOp(Op):
    """This is a helpful operation."""

    def argmap(self) -> list[DArg]:
        return [
            DArg("sym", desc="Symbol name"),
            DArg("points", default=10, desc="Point size"),
            DArg("width", default=0, desc="Width can be zero"),
            DArg("*rest", desc="Extra things"),
        ]

    async def run(self) -> Any:
        return None


def test_help_and_signature_output():
    async def logic():
        d = Dispatch({"align": HelpOp})
        buf = StringIO()
        sink_id = logger.add(buf, level="INFO")
        try:
            await d.runop("align?", None)
        finally:
            logger.remove(sink_id)
        return buf.getvalue()

    text = asyncio.run(logic())

    assert (
        'Command: align <sym> [points default="10"] [width default="0"] [*rest]' in text
    )
    assert "This is a helpful operation" in text
    assert "Default value: 0" in text
