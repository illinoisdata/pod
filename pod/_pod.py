"""
Pod storage interface.
"""

from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

from pathlib import Path
from typing import Dict, Optional, Set

from pod.common import Object, PodId, TimeId, make_pod_id, object_id, step_time_id
from pod.feature import __FEATURE__
from pod.pickling import ManualPodding, PodPickling, StaticPodPickling
from pod.storage import FilePodStorage


class Pod:
    NAMEMAP_OID = 0  # Assume no object resides at this address.

    def __init__(self, pickling: PodPickling) -> None:
        self._pickling = pickling

    def new(self, pod_dir: Path) -> Pod:
        podding_fn = ManualPodding.podding_fn
        post_podding_fn = None
        storage = FilePodStorage(pod_dir)
        pickling = StaticPodPickling(
            storage=storage,
            podding_fn=podding_fn,
            post_podding_fn=post_podding_fn,
        )
        return Pod(pickling)

    def save(self, namespace: Dict[str, Object]) -> TimeId:
        __FEATURE__.new_dump()
        tid = step_time_id()
        namemap_pid = self._save_as_namemap(tid, namespace)
        self._save_objects(tid, namespace)
        return namemap_pid.tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Dict[str, Object]:
        namemap = self._load_namemap(tid)
        if nameset is not None:
            namemap = {name: pid for name, pid in namemap.items() if name in nameset}
        return self._load_objects(namemap)

    def estimate_size(self) -> int:
        return self._pickling.estimate_size()

    def _save_as_namemap(self, tid: TimeId, namespace: Dict[str, Object]) -> PodId:
        namemap_pid = make_pod_id(tid, Pod.NAMEMAP_OID)
        namemap = {name: make_pod_id(tid, object_id(obj)) for name, obj in namespace.items()}
        with self._pickling.dump_batch({namemap_pid: namemap}) as dump_session:
            dump_session.dump(namemap_pid, namemap)
        return namemap_pid

    def _load_namemap(self, tid: TimeId) -> Dict[str, PodId]:
        return self._pickling.load(make_pod_id(tid, Pod.NAMEMAP_OID))

    def _save_objects(self, tid: TimeId, namespace: Dict[str, Object]) -> None:
        podspace = {make_pod_id(tid, object_id(obj)): obj for name, obj in namespace.items()}
        with self._pickling.dump_batch(podspace) as dump_session:
            for pid, obj in podspace.items():  # TODO: Stabilize the order?
                dump_session.dump(pid, obj)

    def _load_objects(self, namemap: Dict[str, PodId]) -> Dict[str, Object]:
        obj_by_pid = self._pickling.load_batch({pid for name, pid in namemap.items()})
        return {name: obj_by_pid[pid] for name, pid in namemap.items()}
