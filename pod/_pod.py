"""
Pod storage interface.
"""

from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

from pathlib import Path
from typing import Dict, Optional, Set, Tuple

from pod.common import Object, PodId, TimeId, make_pod_id, object_id, step_time_id
from pod.feature import __FEATURE__
from pod.pickling import ManualPodding, PodPickling, StaticPodPickling
from pod.storage import FilePodStorage

Namespace = Dict[str, Object]
Namemap = Dict[str, PodId]  # Mapping from variable name to a pod ID.


class PodNamespace(dict):
    def __init__(self, *args, **kwargs) -> None:
        dict.__init__(self, *args, **kwargs)
        self.active_names: Set[str] = set(self.keys())
        self.namemap: Namemap = {}

    def __getitem__(self, name: str) -> Object:
        self.active_names.add(name)
        return dict.__getitem__(self, name)

    def __setitem__(self, name: str, obj: Object) -> None:
        self.active_names.add(name)
        dict.__setitem__(self, name, obj)

    def __delitem__(self, name: str):
        self.active_names.discard(name)

    def __iter__(self):
        self.active_names = set(self.keys())

    def pod_active_names(self) -> Set[str]:
        return self.active_names

    def pod_namemap(self) -> Namemap:
        return self.namemap

    def pod_reset_namemap(self, namemap: Namemap) -> None:
        assert self.keys() == namemap.keys(), f"{self.keys()}, {namemap.keys()}"
        self.namemap = namemap
        self.active_names.clear()


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

    def new_managed_namespace(self, namespace: Namespace = {}) -> PodNamespace:
        return PodNamespace(namespace)

    def save(self, namespace: Namespace) -> TimeId:
        __FEATURE__.new_dump()
        tid = step_time_id()
        namemap_pid, namemap = self._save_as_namemap(tid, namespace)
        self._save_objects(tid, namespace, namemap)
        if isinstance(namespace, PodNamespace):
            namespace.pod_reset_namemap(namemap)
        return namemap_pid.tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        namemap = self._load_namemap(tid)
        if nameset is not None:
            namemap = {name: pid for name, pid in namemap.items() if name in nameset}
        return self._load_objects(namemap)

    def estimate_size(self) -> int:
        return self._pickling.estimate_size()

    def _save_as_namemap(self, tid: TimeId, namespace: Namespace) -> Tuple[PodId, Namemap]:
        namemap_pid = make_pod_id(tid, Pod.NAMEMAP_OID)

        if isinstance(namespace, PodNamespace):
            active_names = namespace.pod_active_names()  # TOFIX: Query connected component.
            prev_namemap = namespace.pod_namemap()
            active_namemap = {
                name: make_pod_id(tid, object_id(namespace[name])) for name, obj in namespace.items() if name in active_names
            }
            inactive_namemap = {name: prev_namemap[name] for name in namespace.keys() if name not in active_names}
            namemap = {**active_namemap, **inactive_namemap}  # Should contain exactly all names in namespace.
        else:
            namemap = {name: make_pod_id(tid, object_id(obj)) for name, obj in namespace.items()}
        with self._pickling.dump_batch({namemap_pid: namemap}) as dump_session:
            dump_session.dump(namemap_pid, namemap)
        return namemap_pid, namemap

    def _load_namemap(self, tid: TimeId) -> Namemap:
        return self._pickling.load(make_pod_id(tid, Pod.NAMEMAP_OID))

    def _save_objects(self, tid: TimeId, namespace: Namespace, namemap: Namemap) -> None:
        podspace = {pid: namespace[name] for name, pid in namemap.items() if pid.tid == tid}
        with self._pickling.dump_batch(podspace) as dump_session:
            for pid, obj in podspace.items():  # TODO: Stabilize the order?
                dump_session.dump(pid, obj)

    def _load_objects(self, namemap: Namemap) -> Namespace:
        obj_by_pid = self._pickling.load_batch({pid for name, pid in namemap.items()})
        return {name: obj_by_pid[pid] for name, pid in namemap.items()}
