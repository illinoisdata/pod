from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np

from pod.common import ObjectId, PodId


def when_enabled(fn):
    def _fn(self, *args, **kwargs):
        if self.is_enabled():
            # print(fn.__name__)
            return fn(self, *args, **kwargs)
        return None

    return _fn


class ChangeTracker:
    def __init__(self) -> None:
        self._prev_pod_set: Set[bytes] = set()
        self._pod_set: Set[bytes] = set()
        self._pod_oids: Dict[PodId, Set[ObjectId]] = {}
        self._pod_sizes: Dict[PodId, Tuple[int, int]] = {}
        self._pod_max_change_prob: Dict[PodId, float] = {}
        self._oid_recent_change: Set[ObjectId] = set()
        self._pod_change: Set[PodId] = set()

        self._oid_count: Dict[ObjectId, int] = {}
        self._oid_change_count: Dict[ObjectId, int] = {}

    def new_dump(self):
        from functools import reduce
        # summarize = lambda x: (reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]), x, (0, 0)), sorted(x, key=lambda y: -y[1]))
        def summarize(x):
            a, b = reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]), x, (0, 0))
            return len(x), a, b / 1e6
        print("STABLE", summarize([v for pid, v in self._pod_sizes.items() if pid not in self._pod_change]))
        print("CHANGE", summarize([v for pid, v in self._pod_sizes.items() if pid in self._pod_change]))
        self._prev_pod_set |= self._pod_set
        # self._pod_set = set()
        self._pod_oids = {}
        self._pod_sizes = {}
        self._oid_recent_change = set()
        self._pod_max_change_prob = {}

    def new_pod_oid(self, pid: PodId, oid: ObjectId):
        if pid not in self._pod_oids:
            self._pod_oids[pid] = set()
        self._pod_oids[pid].add(oid)
        self._pod_max_change_prob[pid] = max(
            self._pod_max_change_prob.get(pid, 0.0),
            self.oid_change_prob(oid),
        )

    def new_pod(self, pid: PodId, pod_bytes: bytes):
        self._pod_set.add(pod_bytes)
        self._pod_sizes[pid] = len(self._pod_oids[pid]), len(pod_bytes)
        # print(pid, self._pod_sizes[pid])
        is_changed = pod_bytes not in self._prev_pod_set
        if is_changed:
            self._pod_change.add(pid)
        for oid in self._pod_oids[pid]:
            if oid not in self._oid_count:
                self._oid_count[oid] = 0
                self._oid_change_count[oid] = 0
            self._oid_count[oid] += 1
            if is_changed:
                self._oid_change_count[oid] += 1
                self._oid_recent_change.add(oid)

    def pod_max_change_prob(self, pid: PodId) -> float:
        return self._pod_max_change_prob.get(pid, 0.0)

    def oid_count(self, oid: ObjectId) -> int:
        return self._oid_count.get(oid, 0)

    def oid_change_count(self, oid: ObjectId) -> int:
        return self._oid_change_count.get(oid, 0)

    def oid_change_prob(self, oid: ObjectId) -> float:
        return self._oid_change_count[oid] / self._oid_count[oid] if oid in self._oid_count else 0.0

    def change_probs_hist(self, bins=10) -> Tuple[List[int], List[float]]:
        change_probs = [self._oid_change_count[oid] / self._oid_count[oid] for oid in self._oid_count]
        counts, bin_lowers = np.histogram(change_probs, bins=10, range=(0, 1))
        return list(counts), list(bin_lowers)

    def has_changed(self, oid: ObjectId) -> bool:
        return oid in self._oid_recent_change


class _Feature:
    def __init__(self, **cfg) -> None:
        self.in_block: bool = False
        self.cfg: Dict[str, Any] = cfg

    def is_enabled(self) -> bool:
        return self.in_block

    def __enter__(self) -> _Feature:
        assert not self.in_block
        self.in_block = True
        self.init_feature()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        assert self.in_block
        self.del_feature()
        self.in_block = False

    """ Feature setup and teardown. """

    def init_feature(self):
        self.time_count = 0
        if self.cfg.get("track_change"):
            self._track_change = ChangeTracker()

    def del_feature(self):
        del self.time_count
        if self.cfg.get("track_change"):
            del self._track_change

    """ Feature collections. """

    @when_enabled
    def new_dump(self):
        self.time_count += 1
        if self.cfg.get("track_change"):
            self._track_change.new_dump()

    @when_enabled
    def new_pod_oid(self, pid: PodId, oid: ObjectId):
        if self.cfg.get("track_change"):
            self._track_change.new_pod_oid(pid, oid)

    @when_enabled
    def new_pod(self, pid: PodId, pod_bytes: bytes):
        if self.cfg.get("track_change"):
            self._track_change.new_pod(pid, pod_bytes)

    """ Feature retrieval. """

    @when_enabled
    def pod_max_change_prob(self, pid: PodId) -> Optional[float]:
        if self.cfg.get("track_change"):
            return self._track_change.pod_max_change_prob(pid)
        return None

    @when_enabled
    def oid_count(self, oid: ObjectId) -> Optional[int]:
        if self.cfg.get("track_change"):
            return self._track_change.oid_count(oid)
        return None

    @when_enabled
    def oid_change_count(self, oid: ObjectId) -> Optional[int]:
        if self.cfg.get("track_change"):
            return self._track_change.oid_change_count(oid)
        return None

    @when_enabled
    def oid_change_prob(self, oid: ObjectId) -> Optional[float]:
        if self.cfg.get("track_change"):
            return self._track_change.oid_change_prob(oid)
        return None

    @when_enabled
    def change_probs_hist(self, bins=10) -> Optional[Tuple[List[int], List[float]]]:
        if self.cfg.get("track_change"):
            return self._track_change.change_probs_hist(bins=bins)
        return None

    @when_enabled
    def has_changed(self, oid: ObjectId) -> Optional[bool]:
        if self.cfg.get("track_change"):
            return self._track_change.has_changed(oid)
        return None


__FEATURE__ = _Feature(
    track_change=True,
)


if __name__ == "__main__":
    print(__FEATURE__.is_enabled())
    with __FEATURE__ as feature:
        print(feature, __FEATURE__, __FEATURE__.is_enabled())
        __FEATURE__.new_dump()
        __FEATURE__.new_pod_oid(PodId(1, 1), 1)
        __FEATURE__.new_pod_oid(PodId(1, 1), 2)
        __FEATURE__.new_pod(PodId(1, 1), b"123")
        __FEATURE__.new_dump()
        __FEATURE__.new_pod_oid(PodId(1, 1), 1)
        __FEATURE__.new_pod(PodId(1, 1), b"123")
        __FEATURE__.new_pod_oid(PodId(1, 2), 2)
        __FEATURE__.new_pod(PodId(1, 2), b"3")
        print(f"{__FEATURE__.oid_change_count(1)} / {__FEATURE__.oid_count(1)}")
        print(f"{__FEATURE__.oid_change_count(2)} / {__FEATURE__.oid_count(2)}")
    print(__FEATURE__.is_enabled())
