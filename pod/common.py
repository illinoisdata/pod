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


def object_id(obj: Object) -> ObjectId:
    return id(obj)


def make_pod_id(tid: TimeId, oid: ObjectId) -> PodId:
    return PodId(tid=tid, oid=oid)


@dataclass
class PodDependency:
    dep_pids: Set[PodId]  # List of pids this pod depends on.
    rank: Rank  # Rank of the pod for sorting.
    meta: bytes  # Serialized metadata.

    def __reduce__(self):
        return self.__class__, (self.dep_pids, self.rank, self.meta)


def plot_deps(deps: Dict[PodId, PodDependency]):
    ps = graphviz.Digraph()
    for pid in deps:
        ps.node(str(pid))
    for pid, dep in deps.items():
        for dep_pid in dep.dep_pids:
            ps.edge(str(pid), str(dep_pid))
    ps.view()
