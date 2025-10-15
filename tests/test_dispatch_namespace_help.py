import asyncio

from mutil.dispatch import Dispatch, Op


class A(Op):
    async def run(self):
        return "A"


def test_global_help_lists_groups(capfd):
    async def logic():
        d = Dispatch({"grp": {"x": A}, "y": A})
        # Ask for global help
        await d.runop("?")

    asyncio.run(logic())
    out, _ = capfd.readouterr()
    # Outputs all commands; make sure grouped section is present
    assert "grp:" in out or "Other:" in out


