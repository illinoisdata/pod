# From https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python
# TODO: Turns this into C extension for speed?
import sys
from collections import deque
from collections.abc import Mapping, Set
from numbers import Number

ZERO_DEPTH_BASES = (str, bytes, Number, range, bytearray, type)


def recursive_size(obj_0):
    """Recursively iterate to sum size of object & members."""
    _seen_ids = set()

    def inner(obj):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0
        _seen_ids.add(obj_id)
        size = sys.getsizeof(obj)
        try:
            if isinstance(obj, ZERO_DEPTH_BASES):
                pass  # bypass remaining control flow and return
            elif isinstance(obj, (tuple, list, Set, deque)):
                size += sum(inner(i) for i in obj)
            elif isinstance(obj, Mapping):
                size += sum(inner(k) + inner(v) for k, v in getattr(obj, "items")())
            # Check for custom object instances - may subclass above too
            if hasattr(obj, "__dict__"):
                size += inner(vars(obj))
            if hasattr(obj, "__slots__") and obj.__slots__ is not None:  # can have __slots__ with __dict__
                size += sum(inner(getattr(obj, s)) for s in obj.__slots__ if hasattr(obj, s))
        except Exception:
            pass
        return size

    return inner(obj_0)