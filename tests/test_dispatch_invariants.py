import pytest

from mutil.dispatch import DArg


def test_state_name_forbidden():
    with pytest.raises(AssertionError):
        DArg("state")


def test_empty_name_behaviour():
    # Current behavior: allowed but should be used carefully. Just ensure it constructs.
    a = DArg("")
    assert a.name == ""


