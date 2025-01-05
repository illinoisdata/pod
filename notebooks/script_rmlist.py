"""Script version of rmlist."""

import secrets
import random


def f(x):  # Test pickling functions.
    def g():
        return h()
    return g() if x else 0


def h():
    return f(False)


def save():
    z = secrets.token_bytes(1000 * 10000)
    __pod_save__()


lc = secrets.token_bytes(1000 * 100000)
l = [
    secrets.token_bytes(100000)
    for idx in range(1000)
]
l_share = l + [  # Sharing immutable data with mutating list.
    secrets.token_bytes(100000)
    for idx in range(1000)
]
l2 = [l]; l.append(l2)  # Test self-referential objects.
a = []; b1 = [a]; b2 = [a]


__pod_save__()


for cdx in range(10 - 1):
    for idx in random.sample(range(len(l) - 1), 10):
        l[idx] = secrets.token_bytes(100000)
    __pod_save__()
    a.append(idx)
    save()
