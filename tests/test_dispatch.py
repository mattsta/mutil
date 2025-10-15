import unittest
from typing import Any

from mutil.dispatch import DArg, Dispatch, Op


class SampleOp(Op):
    def argmap(self) -> list[DArg]:
        return [
            DArg("arg1"),
            DArg("arg2", default="default_value"),
            DArg("*rest"),
        ]

    async def run(self) -> Any:
        return self.arg1, self.arg2, self.rest


class TestDispatch(unittest.TestCase):
    def test_command_dispatch(self):
        async def test_logic():
            dispatch = Dispatch({"test": SampleOp})

            # Test with all arguments
            result = await dispatch.runop("test", "val1 val2 val3 val4")
            self.assertEqual(result, ("val1", "val2", ["val3", "val4"]))

            # Test with required arguments only
            result = await dispatch.runop("test", "val1")
            self.assertEqual(result, ("val1", "default_value", []))

            # Test with missing required arguments
            result = await dispatch.runop("test", "")
            self.assertIsNone(result)

        import asyncio

        asyncio.run(test_logic())

    def test_invalid_default_ordering(self):
        class BadOrderingOp(Op):
            def argmap(self) -> list[DArg]:
                return [
                    DArg("a"),
                    DArg("b", default=1),  # default in the middle
                    DArg("c"),  # required after a default -> invalid
                ]

            async def run(self) -> Any:
                return None

        async def test_logic():
            dispatch = Dispatch({"bad": BadOrderingOp})
            # Should fail validation immediately
            result = await dispatch.runop("bad", "x")
            self.assertIsNone(result)

        import asyncio

        asyncio.run(test_logic())


if __name__ == "__main__":
    unittest.main()
