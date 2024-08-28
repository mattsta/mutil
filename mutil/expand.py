"""String expansion helpers!"""

import itertools
import re

from typing import Iterable


def expand_string_numeric(part: str) -> Iterable[str]:
    """Return an iterable of integer strings if provided in the form A..B.

    Example: expand_string_numeric("5..10") -> ['5', '6', '7', '8', '9', '10']
    Example: expand_string_numeric("10..5") -> ['10', '9', '8', '7', '6', '5']
    """

    # split into sections BEFORE number processing
    if ".." in part:
        start, end = map(int, part.split(".."))
        step = -1 if start > end else 1
        return map(str, range(start, end + step, step))

    # else, no number expansion detected, so return individual value wrapped in
    # a container for the cross-product to apply against other elements.
    return [part]


def expand_string_curly_braces(s) -> list[str]:
    """Expand a single input string having curly brace replacements into a list of strings.

    Usage: expand_string_curly_braces("Hello My Name is {Glorbo,Rambo,Crambo}") generates
           ["Hello My Name is Glorbo",
            "Hello My Name is Rambo",
            "Hello My Name is Crambo"]

    Also allows expansion of numeric sequences in {A..B} and {B..A} forms and also allows
    combining numeric expansion and string expansion/duplication in the same bracket block.

    See tests/test_expand.py for more examples.

    """

    # extract string into parts from inside {} and parts not inside {}.
    # the 'parts' here alternates "Bare strings" vs "strings from inside brackets" so
    # every OTHER element of 'parts' will be sourced from inside {} (if any).
    parts = re.split(r"{([^{}]*)}", s)

    expanded = []
    for i, p in enumerate(parts):
        # EVEN FIELDS are ALWAYS an original bare string with no curly brace expansion
        if i % 2 == 0:
            # Note: we add as [p] because we run PRODUCT against 'expanded' so we need
            #       this to be an iterable-of-content to cross-productize later.
            expanded.append([p])
        else:
            # ODD FIELDS are ALWAYS {} entries with contents to decode.

            # Because we cross-product these results, we need to assemble an inner
            # list of extracted matches so they get applied evenly over the
            # non-self-enumerating parts.
            inner_parts: list[str] = []
            for ps in p.split(","):
                found = expand_string_numeric(ps)
                # we add EACH EXTRACTED GROUP as individual elements to the inner result list.
                # (this 'extend' operation also iterates any map() returned from the number expansion)
                inner_parts.extend(found)
            else:
                # now we add all the INDIVIDUAL INNER PARTS as a WHOLE LIST to the final result for combining
                # (so 'expanded' is a list of lists/iterables where each inner list has strings to join)
                expanded.append(inner_parts)

    output = ["".join(p) for p in itertools.product(*expanded)]
    return output
