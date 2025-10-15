import pytest

from mutil.dispatch import DArg


def test_darg_required_pass_through():
    a = DArg("x")
    assert a.validate("42") is True
    assert a.val == "42"


def test_darg_convert_success():
    a = DArg("x", convert=int)
    assert a.validate("7") is True
    assert a.val == 7


def test_darg_convert_exception_returns_false():
    def bad(x):
        raise ValueError("boom")

    a = DArg("x", convert=bad)
    assert a.validate("7") is False


def test_darg_verify_success_and_failure():
    a = DArg("x", convert=int, verify=lambda v: v > 5)
    assert a.validate("6") is True
    assert a.val == 6

    b = DArg("y", convert=int, verify=lambda v: v > 5)
    assert b.validate("4") is False


def test_darg_verify_exception_returns_false():
    def bad_verify(_):
        raise RuntimeError("nope")

    a = DArg("x", convert=int, verify=bad_verify)
    assert a.validate("10") is False


@pytest.mark.parametrize("default_value", ["", 0, False])
def test_darg_defaults_applied_for_none_even_if_falsy(default_value):
    a = DArg("x", default=default_value)
    assert a.validate(None) is True
    assert a.val is default_value


def test_isrest_and_usename():
    r = DArg("*items")
    assert r.isRest() is True
    assert r.usename() == "items"

    n = DArg("name")
    assert n.isRest() is False
    assert n.usename() == "name"
