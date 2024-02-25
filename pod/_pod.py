"""
Pod storage interface.
"""

from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

from pathlib import Path
from typing import Dict, Optional, Set, Tuple

from pod.common import Object, PodId, TimeId, make_pod_id, object_id, step_time_id
from pod.feature import __FEATURE__
from pod.pickling import ManualPodding, PodPickling, SnapshotPodPickling, StaticPodPickling
from pod.storage import FilePodStorage

Namespace = Dict[str, Object]
Namemap = Dict[str, PodId]  # Mapping from variable name to a pod ID.


class ObjectStorage:
    def new_managed_namespace(self, namespace: Namespace = {}) -> Namespace:
        return namespace

    def save(self, namespace: Namespace) -> TimeId:
        raise NotImplementedError("Abstract method")

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        raise NotImplementedError("Abstract method")

    def estimate_size(self) -> int:
        raise NotImplementedError("Abstract method")


class SnapshotObjectStorage(ObjectStorage):
    SNAPSHOT_OID = 0  # Assume no object resides at this address.

    def __init__(self, pickling: PodPickling) -> None:
        self._pickling = pickling

    def new(self, pod_dir: Path) -> SnapshotObjectStorage:
        return SnapshotObjectStorage(SnapshotPodPickling(pod_dir))

    def save(self, namespace: Namespace) -> TimeId:
        tid = step_time_id()
        pid = make_pod_id(tid, SnapshotObjectStorage.SNAPSHOT_OID)
        with self._pickling.dump_batch({pid: namespace}) as dump_session:
            dump_session.dump(pid, namespace)
        return tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        namespace = self._pickling.load(make_pod_id(tid, SnapshotObjectStorage.SNAPSHOT_OID))
        if nameset is not None:
            namespace = {name: namespace[name] for name in nameset}
        return namespace

    def estimate_size(self) -> int:
        return self._pickling.estimate_size()


# TODO: Filter read-only variables in function scope dict better.
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
        return dict.__setitem__(self, name, obj)

    def __delitem__(self, name: str):
        self.active_names.discard(name)
        return dict.__delitem__(self, name)

    def items(self):
        self.active_names = set(self.keys())  # TODO: Use enum for this.
        return dict.items(self)

    def pod_active_names(self) -> Set[str]:
        return self.active_names

    def pod_namemap(self) -> Namemap:
        return self.namemap

    def pod_reset_namemap(self, namemap: Namemap) -> None:
        assert self.keys() == namemap.keys(), f"{self.keys()}, {namemap.keys()}"
        self.namemap = namemap
        self.active_names.clear()


class PodObjectStorage(ObjectStorage):
    NAMEMAP_OID = 0  # Assume no object resides at this address.

    def __init__(self, pickling: PodPickling) -> None:
        self._pickling = pickling

    def new(self, pod_dir: Path) -> PodObjectStorage:
        podding_fn = ManualPodding.podding_fn
        post_podding_fn = None
        storage = FilePodStorage(pod_dir)
        pickling = StaticPodPickling(
            storage=storage,
            podding_fn=podding_fn,
            post_podding_fn=post_podding_fn,
        )
        return PodObjectStorage(pickling)

    def new_managed_namespace(self, namespace: Namespace = {}) -> Namespace:
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
        namemap_pid = make_pod_id(tid, PodObjectStorage.NAMEMAP_OID)

        if isinstance(namespace, PodNamespace):
            active_names = self._connected_active_names(tid, namespace)
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
        return self._pickling.load(make_pod_id(tid, PodObjectStorage.NAMEMAP_OID))

    def _save_objects(self, tid: TimeId, namespace: Namespace, namemap: Namemap) -> None:
        podspace = {pid: namespace[name] for name, pid in namemap.items() if pid.tid == tid}
        with self._pickling.dump_batch(podspace) as dump_session:
            for pid, obj in podspace.items():  # TODO: Stabilize the order?
                dump_session.dump(pid, obj)

    def _load_objects(self, namemap: Namemap) -> Namespace:
        obj_by_pid = self._pickling.load_batch({pid for name, pid in namemap.items()})
        return {name: obj_by_pid[pid] for name, pid in namemap.items()}

    def _connected_active_names(self, tid: TimeId, namespace: PodNamespace) -> Set[str]:
        active_names = namespace.pod_active_names()
        prev_namemap = namespace.pod_namemap()

        # Find union find roots of all pids.
        prev_pids = {pid for _, pid in prev_namemap.items()}
        connected_roots = self._pickling.connected_pods(prev_pids)

        # Filter only pids that share root with active names.
        active_roots = {connected_roots[prev_namemap[name]] for name in active_names if name in prev_namemap}
        return {  # Get connected active names.
            name
            for name in namespace.keys()
            if name not in prev_namemap or connected_roots[prev_namemap[name]] in active_roots
        }
