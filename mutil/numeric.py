"""Numeric manipulation and printing"""

from decimal import Decimal
from enum import Enum

ROUND = Enum("ROUND", "UP DOWN NEAR")


def roundnear(
    increment: Decimal | float,
    price: Decimal | float,
    direction: ROUND = ROUND.NEAR,
    decimals: int = 2,
) -> Decimal:
    """Round a float to a multiple of 'increment'.

    This is a more adjustable version of just:
        # example for enforcing 0.25 increments:
        return round(round(x * 4) / 4, 2)

    Note: 0.00 is a valid multiple if rounding down from near-zero or rounding
          up from a negative near-zero."""

    if not isinstance(price, Decimal):
        price = Decimal(str(price))

    if not isinstance(increment, Decimal):
        increment = Decimal(str(increment))

    # price already ends in the target round increment, so input price already works;
    # just stop and return original (*NOW Decimal*) price here.
    rem: Decimal = price.remainder_near(increment)
    if rem == 0:
        return price

    # else, we need to do some rounding to comply with increment.
    # NOTE: for now, python evaluates 'match' directives sequentially
    #       (like one big "if/else" statement), so the higher the case check
    #       in a match statement, the faster is is reached.
    #       From benchmarking, each additional un-matched 'case' hop is an additional
    #       20 ns of processing in the Python 3.12 VM (but it was 70 ns per case hop in Python 3.10!).
    #       Technically, just using if/else branches are 2 ns faster per case, but this still looks cleaner,
    #       so I'll accept the additional 2 ns to 6 ns per match statement penalty for readability.
    match direction:
        case ROUND.NEAR:
            # Decimal().remainder_near() returns:
            #  - NEGATIVE VALUES when the CLOSEST next value is HIGHER
            #  - POSITIVE VALUES when the CLOSEST next value is LOWER
            if rem < 0:
                # lower remainder means we are closer to the next higher increment,
                # so round UP
                price = price + (increment / 2)
            else:
                # higher remainder means we are closer to the previous lower increment,
                # so round DOWN
                price = price - (increment / 2)
        case ROUND.UP:
            price = price + (increment / 2)
        case ROUND.DOWN:
            price = price - (increment / 2)

    # move rounded price to the increment modulus
    finalRemainder = price.remainder_near(increment)
    return round(price - finalRemainder, decimals)


def roundnear5(price: float, direction: ROUND) -> Decimal:
    """Round 'price' to end in a multiple of '0.05'"""
    return roundnear(0.05, price, direction)


def roundnear10(price: float, direction: ROUND) -> Decimal:
    """Round 'price' to end in a multiple of '0.10'"""
    return roundnear(0.10, price, direction)


def fmtPrice(x, decimals=4) -> str:
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


def fmtPricePad(n, padding=10, decimals=4) -> str:
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

    # print at most N digits, not ending in zeroes...
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
