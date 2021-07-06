import mutil.numeric as n


def test_rn5_up():
    """ Round up """
    assert n.roundnear5(0.24, True) == 0.25
    assert n.roundnear5(0.01, True) == 0.05
    assert n.roundnear5(-0.01, True) == 0.00


def test_rn5_down():
    """ Round down """
    assert n.roundnear5(0.24, False) == 0.20
    assert n.roundnear5(0.01, False) == 0.00
    assert n.roundnear5(-0.01, False) == -0.05


def test_rn10_up():
    """ Round up """
    assert n.roundnear10(0.24, True) == 0.30
    assert n.roundnear10(0.01, True) == 0.10
    assert n.roundnear10(-0.01, True) == 0.00


def test_rn10_down():
    """ Round down """
    assert n.roundnear10(0.24, False) == 0.20
    assert n.roundnear10(0.01, False) == 0.00
    assert n.roundnear10(-0.01, False) == -0.10


def roundnear7(p, updown):
    """ Rounds near a *multiple* of 0.07, which may not be a number ending in .07"""
    return n.roundnear(0.07, p, updown)


def test_rn7_up():
    """ Round up """
    assert roundnear7(0.24, True) == 0.28
    assert roundnear7(0.01, True) == 0.07
    assert roundnear7(-0.01, True) == 0.00


def test_rn7_down():
    """ Round down """
    assert roundnear7(0.24, False) == 0.21
    assert roundnear7(0.01, False) == 0.00
    assert roundnear7(-0.01, False) == -0.07


def roundnear1xx(p, updown):
    """ Rounds near the next whole dollar increment, no cents. """
    return n.roundnear(1, p, updown)


def test_rn1xx_up():
    """ Round up """
    assert roundnear1xx(0.24, True) == 1
    assert roundnear1xx(700.23, True) == 701
    assert roundnear1xx(-0.01, True) == 0.00


def test_rn1xx_down():
    """ Round down """
    assert roundnear1xx(0.24, False) == 0.00
    assert roundnear1xx(700.23, False) == 700
    assert roundnear1xx(-0.01, False) == -1


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
        assert n.fmtPricePad(-a, 0) == "-" + b
        assert n.fmtPrice(-a) == "-" + b
