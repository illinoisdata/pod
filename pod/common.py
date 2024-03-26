from __future__ import annotations  # isort:skip

from dataclasses import dataclass
from typing import Any, Dict, Set

import graphviz

Object = Any
ObjectId = int
TimeId = int
Rank = int


@dataclass(frozen=True)
class PodId:
    tid: TimeId
    oid: ObjectId

    def __reduce__(self):
        return self.__class__, (self.tid, self.oid)

    def __eq__(self, other: object):
        return isinstance(other, PodId) and self.tid == other.tid and self.oid == other.oid

    def __hash__(self):
        # Cantor pairing function: (a + b) * (a + b + 1) / 2 + a; where a, b >= 0
        return (self.tid + self.oid) * (self.tid + self.oid + 1) // 2 + self.tid
        # Szudzik's function: a >= b ? a * a + a + b : a + b * b;  where a, b >= 0
        # return (
        #     self.tid + self.oid * self.oid
        #     if self.tid < self.oid
        #     else self.tid * self.tid + self.tid + self.oid
        # )

    def redis_str(self) -> str:
        return str(self.tid) + "|" + str(self.oid)

    @staticmethod
    def from_redis_str(redis_str):
        tid_s, oid_s = redis_str.split("|")
        return PodId(TimeId(tid_s), ObjectId(oid_s))


_current_tid = 0


def step_time_id() -> TimeId:
    global _current_tid
    tid = _current_tid
    _current_tid += 1
    return tid


_current_rank = 0


def next_rank() -> Rank:
    global _current_rank
    rank = _current_rank
    _current_rank += 1
    return rank


@dataclass
class PodDependency:
    dep_pids: Set[PodId]  # List of pids this pod depends on.
    rank: Rank  # Rank of the pod for sorting.
    meta: bytes  # Serialized metadata.
    immutable: bool  # Does this pod contain immutable object(s)?

    def __reduce__(self):
        return self.__class__, (self.dep_pids, self.rank, self.meta, self.immutable)


def plot_deps(deps: Dict[PodId, PodDependency]):
    ps = graphviz.Digraph()
    for pid in deps:
        ps.node(str(pid))
    for pid, dep in deps.items():
        for dep_pid in dep.dep_pids:
            ps.edge(str(pid), str(dep_pid))
    ps.view()
