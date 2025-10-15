import asyncio

from mutil.dispatch import Dispatch, Op


class A(Op):
    async def run(self):
        return "alpha"


class Al(Op):
    async def run(self):
        return "alpine"


def test_ambiguous_prefix_prints_completions(capfd):
    async def logic():
        d = Dispatch({"alpha": A, "alpine": Al})
        await d.runop("al")

    asyncio.run(logic())
    out, _ = capfd.readouterr()
    assert "Completion choices:" in out
    assert "alpha" in out and "alpine" in out


def test_exact_full_name_executes_even_with_ambiguous_prefix():
    async def logic():
        d = Dispatch({"alpha": A, "alpine": Al})
        assert await d.runop("alpha") == "alpha"

    asyncio.run(logic())


def test_adding_command_that_introduces_ambiguity_does_not_break_exact():
    async def logic():
        d = Dispatch({"alpha": A})
        # exact works
        assert await d.runop("alpha") == "alpha"

        # introduce ambiguity by constructing a new dispatch table
        d2 = Dispatch({"alpha": A, "alpine": Al})
        assert await d2.runop("alpha") == "alpha"

    asyncio.run(logic())


