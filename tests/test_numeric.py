from decimal import Decimal

import mutil.numeric as n
from mutil.numeric import ROUND


def test_noround():
    """Round on boundary doesn't change"""

    # we had errors where rounding on small numbers exactly
    # on a matching round condition still caused the rounding
    # logic to change the input value (which was very bad!)
    for p in (0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35):
        assert n.roundnear(0.05, p, ROUND.UP) == Decimal(str(Decimal(str(p))))
        assert n.roundnear(0.05, p, ROUND.DOWN) == Decimal(str(Decimal(str(p))))


def test_rn5_up():
    """Round up"""
    assert n.roundnear5(0.24, ROUND.UP) == Decimal(str(0.25))
    assert n.roundnear5(0.01, ROUND.UP) == Decimal(str(0.05))
    assert n.roundnear5(-0.01, ROUND.UP) == Decimal(str(0.00))


def test_rn5_down():
    """Round down"""
    assert n.roundnear5(0.24, ROUND.DOWN) == Decimal(str(0.20))
    assert n.roundnear5(0.01, ROUND.DOWN) == Decimal(str(0.00))
    assert n.roundnear5(-0.01, ROUND.DOWN) == Decimal(str(-0.05))


def test_rn10_up():
    """Round up"""
    assert n.roundnear10(0.24, ROUND.UP) == Decimal(str(0.30))
    assert n.roundnear10(0.01, ROUND.UP) == Decimal(str(0.10))
    assert n.roundnear10(-0.01, ROUND.UP) == Decimal(str(0.00))


def test_rn10_down():
    """Round down"""
    assert n.roundnear10(0.24, ROUND.DOWN) == Decimal(str(0.20))
    assert n.roundnear10(0.01, ROUND.DOWN) == Decimal(str(0.00))
    assert n.roundnear10(-0.01, ROUND.DOWN) == Decimal(str(-0.10))


def roundnear7(p, updown):
    """Rounds near a *multiple* of 0.07, which may not be a number ending in .07"""
    return n.roundnear(0.07, p, updown)


def test_roundnear025():
    assert n.roundnear(0.025, 1.025, ROUND.UP, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.026, ROUND.UP, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.026, ROUND.DOWN, 3) == Decimal(str(1.025))


def test_roundnear025_NEAR():
    assert n.roundnear(0.025, 1.025, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.026, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.027, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.028, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.029, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.030, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.031, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.032, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.033, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.034, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.035, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.036, ROUND.NEAR, 3) == Decimal(str(1.025))
    assert n.roundnear(0.025, 1.037, ROUND.NEAR, 3) == Decimal(str(1.025))
    # switchover between rounding down and rounding up
    assert n.roundnear(0.025, 1.038, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.039, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.040, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.041, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.042, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.043, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.044, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.045, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.046, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.047, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.048, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.049, ROUND.NEAR, 3) == Decimal(str(1.050))
    assert n.roundnear(0.025, 1.050, ROUND.NEAR, 3) == Decimal(str(1.050))


def test_roundnear025_under():
    assert n.roundnear(0.025, -1.025, ROUND.UP, 3) == Decimal(str(-1.025))
    assert n.roundnear(0.025, -1.026, ROUND.UP, 3) == Decimal(str(-1.025))
    assert n.roundnear(0.025, -1.026, ROUND.DOWN, 3) == Decimal(str(-1.050))


def test_rn7_up():
    """Round up"""
    assert roundnear7(0.24, ROUND.UP) == Decimal(str(0.28))
    assert roundnear7(0.01, ROUND.UP) == Decimal(str(0.07))
    assert roundnear7(-0.01, ROUND.UP) == Decimal(str(0.00))


def test_rn7_down():
    """Round down"""
    assert roundnear7(0.24, ROUND.DOWN) == Decimal(str(0.21))
    assert roundnear7(0.01, ROUND.DOWN) == Decimal(str(0.00))
    assert roundnear7(-0.01, ROUND.DOWN) == Decimal(str(-0.07))


def roundnear1xx(p, updown):
    """Rounds near the next whole dollar increment, no cents."""
    return n.roundnear(1, p, updown)


def test_rn1xx_up():
    """Round up"""
    assert roundnear1xx(0.24, ROUND.UP) == Decimal(str(1))
    assert roundnear1xx(700.23, ROUND.UP) == Decimal(str(701))
    assert roundnear1xx(-0.01, ROUND.UP) == Decimal(str(0.00))


def test_rn1xx_down():
    """Round down"""
    assert roundnear1xx(0.24, ROUND.DOWN) == Decimal(str(0.00))
    assert roundnear1xx(700.23, ROUND.DOWN) == Decimal(str(700))
    assert roundnear1xx(-0.01, ROUND.DOWN) == Decimal(str(-1))


def test_fmtBasic():
    checks = [
        (3.333333333333, "3.3333"),
        (1.24, "1.24"),
        (0.6791334, "0.6791"),
        (800000.333, "800,000.333"),
        (800000222222.333777777777777, "800,000,222,222.3337"),
        (9000.2211999999, "9,000.2212"),
    ]

    for a, b in checks:
        # all our test input values must be positive because we turn them
        # negative automatically for more test cases.
        assert a > 0

        assert n.fmtPricePad(a, 0) == b
        assert n.fmtPrice(a) == b

        # test negative versions too
        assert n.fmtPricePad(-a, 0) == ("-" + b)
        assert n.fmtPrice(-a) == ("-" + b)
