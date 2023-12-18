from dataclasses import dataclass
from typing import Any, Dict, Set

import graphviz

Object = Any
ObjectId = int
TimeId = int


@dataclass(frozen=True)
class PodId:
    tid: TimeId
    oid: ObjectId


_current_tid = 0


def step_time_id() -> TimeId:
    global _current_tid
    tid = _current_tid
    _current_tid += 1
    return tid


def object_id(obj: Object) -> ObjectId:
    return id(obj)


def make_pod_id(tid: TimeId, oid: ObjectId) -> PodId:
    return PodId(tid=tid, oid=oid)


def plot_deps(deps: Dict[PodId, Set[PodId]]):
    ps = graphviz.Digraph()
    for pid in deps:
        ps.node(str(pid))
    for pid, dep_pids in deps.items():
        for dep_pid in dep_pids:
            ps.edge(str(pid), str(dep_pid))
    ps.view()
