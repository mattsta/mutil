""" Numeric manipulation and printing """

from decimal import Decimal


def roundnear(end: float, price: float, roundUp: bool) -> float:
    """Round a float to a multiple of 'end'.

    This is a more adjustable version of just:
        # example for enforcing 0.25 increments:
        return round(round(x * 4) / 4, 2)

    Note: 0.00 is a valid multiple if rounding down from near-zero or rounding
          up from a negative near-zero."""

    # Borrowed from https://stackoverflow.com/questions/13106953/python-round-05up
    p = Decimal(price)
    dend = Decimal(end)
    remainder = p.remainder_near(dend)

    # already ends in the target round increment
    if remainder == 0:
        return price

    # or maybe we want round up, so round to nearest upper breach of increment
    if roundUp:
        p = Decimal(price + (end / 2))
    else:
        # else, smaller
        p = Decimal(price - (end / 2))

    remainder = p.remainder_near(dend)
    return round(float(p - remainder), 2)


def roundnear5(price: float, roundUp: bool) -> float:
    """Round 'price' to end in a multiple of '0.05'"""
    return roundnear(0.05, price, roundUp)


def roundnear10(price: float, roundUp: bool) -> float:
    """Round 'price' to end in a multiple of '0.10'"""
    return roundnear(0.10, price, roundUp)


def fmtPrice(x, decimals=4):
    """Print price with minimum of two digit decimals or maximum
    of 4 decimals.

    Any rounding for the decimal cutoff is done via the .g formatter directly.
    """
    sign, digits, el = Decimal(str(x)).as_tuple()

    # nan, just be yourself...
    if el == "n":
        return x

    # guard the formatting lengths against being 0 because python
    # throws an exception if you try to format with a 0 specifier.
    capLen = max(2, min(decimals, abs(el)))
    finalLen = max(1, capLen + (len(digits) - abs(el)))

    if abs(el) < 3:
        return f"{x:,.2f}"

    return f"{x:,.{finalLen}g}"


def fmtPricePad(n, padding=10, decimals=4):
    """Format price with padding as:
    - two decimal precision if <= 2 decimals
        - i.e. print 120 as 120.00
               print 120.3 as 120.30
    - up to 4 decimal places if more than 2 (as formatted by 'g')

    Rounding is done by round() and Decimal() then printed via .f or .g

    Also has a check to ignore very small values taking up wide space."""

    # python formatting doesn't allow 0 padding
    padding = max(padding, 1)

    # don't show tiny tiny values, and use 'abs' because sometimes
    # we're printing full negative percentages like -20.3242343289%, which is
    # les sthan 0.005, but we want to still print them nicely.
    # also guard against 'n' being None for some reason.
    if (not n) or abs(n) < 0.005:
        if n:
            return f"{n:>{padding},.2f}"

        return f"{' ':>{padding}}"

    # print at most 4 digits, not ending in zeroes...
    dn = Decimal(str(round(n, decimals)))
    sign, digits, exponent = dn.as_tuple()

    # skip formatting nan
    if exponent == "n":
        return f"{dn:>{padding}}"

    # if less than 3 decimals, always use two places.
    if abs(exponent) < 3:
        return f"{dn:>{padding},.2f}"

    # this long G is okay because we already truncated
    # the float value to 4 decimals at most using the triple conversion
    # above of Decimal(str(round(x, 4)))
    return f"{dn:>{padding},.20g}"
