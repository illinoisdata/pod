"""
Pod storage interface.
"""

from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from pod.common import Object, PodId, TimeId, step_time_id
from pod.pickling import ManualPodding, PodPickling, SnapshotPodPickling, StaticPodPickling
from pod.stats import ExpStat
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

    def join(self) -> None:
        pass

    def instrument(self, expstat: Optional[ExpStat]) -> None:
        pass


""" Snapshot namespace storage """


class SnapshotObjectStorage(ObjectStorage):
    SNAPSHOT_OID = 0  # Assume no object resides at this address.

    def __init__(self, pickling: PodPickling) -> None:
        self._pickling = pickling

    def new(self, pod_dir: Path) -> SnapshotObjectStorage:
        return SnapshotObjectStorage(SnapshotPodPickling(pod_dir))

    def save(self, namespace: Namespace) -> TimeId:
        tid = step_time_id()
        pid = PodId(tid, SnapshotObjectStorage.SNAPSHOT_OID)
        with self._pickling.dump_batch({pid: namespace}) as dump_session:
            dump_session.dump(pid, namespace)
        return tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        namespace = self._pickling.load(PodId(tid, SnapshotObjectStorage.SNAPSHOT_OID))
        if nameset is not None:
            namespace = {name: namespace[name] for name in nameset}
        return namespace

    def estimate_size(self) -> int:
        return self._pickling.estimate_size()


""" Pod namespace storage """


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
        # assert self.keys() == namemap.keys(), f"{self.keys()}, {namemap.keys()}"
        self.namemap = namemap
        self.active_names.clear()


class PodObjectStorage(ObjectStorage):
    NAMEMAP_OID = 0  # Assume no object resides at this address.

    def __init__(self, pickling: PodPickling) -> None:
        self._pickling = pickling

    @staticmethod
    def new(pod_dir: Path) -> PodObjectStorage:
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
        tid = step_time_id()
        namemap_pid, namemap = self._save_as_namemap(tid, namespace)
        self._save_objects(tid, namespace, namemap_pid, namemap)
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
        namemap_pid = PodId(tid, PodObjectStorage.NAMEMAP_OID)

        if isinstance(namespace, PodNamespace):
            active_names = self._connected_active_names(tid, namespace)
            prev_namemap = namespace.pod_namemap()
            active_namemap = {
                name: PodId(tid, id(dict.__getitem__(namespace, name))) for name in namespace.keys() if name in active_names
            }
            inactive_namemap = {name: prev_namemap[name] for name in namespace.keys() if name not in active_names}
            namemap = {**active_namemap, **inactive_namemap}  # Should contain exactly all names in namespace.
        else:
            namemap = {name: PodId(tid, id(obj)) for name, obj in namespace.items()}
        return namemap_pid, namemap

    def _load_namemap(self, tid: TimeId) -> Namemap:
        return self._pickling.load(PodId(tid, PodObjectStorage.NAMEMAP_OID))

    def _save_objects(self, tid: TimeId, namespace: Namespace, namemap_pid: PodId, namemap: Namemap) -> None:
        podspace = {pid: dict.__getitem__(namespace, name) for name, pid in namemap.items() if pid.tid == tid}
        with self._pickling.dump_batch({namemap_pid: namemap_pid, **podspace}) as dump_session:
            dump_session.dump(namemap_pid, namemap)
            for pid, obj in podspace.items():  # TODO: Stabilize the order?
                dump_session.dump(pid, obj)

    def _load_objects(self, namemap: Namemap) -> Namespace:
        obj_by_pid = self._pickling.load_batch({pid for name, pid in namemap.items()})
        return {name: obj_by_pid[pid] for name, pid in namemap.items()}

    def _connected_active_names(self, tid: TimeId, namespace: PodNamespace) -> Set[str]:
        active_names = namespace.pod_active_names()
        prev_namemap = namespace.pod_namemap()

        # Find union find roots of all pids.
        connected_roots = self._pickling.connected_pods()

        # Filter only pids that share root with active names.
        active_roots = {
            connected_roots.get(prev_namemap[name], prev_namemap[name]) for name in active_names if name in prev_namemap
        }
        return {  # Get connected active names.
            name
            for name in namespace.keys()
            if name not in prev_namemap or connected_roots.get(prev_namemap[name], prev_namemap[name]) in active_roots
        }


""" Asynchronous pod namespace storage """


class LockIf:
    def __init__(self, lock: Optional[threading.Lock], expstat: Optional[ExpStat]) -> None:
        self._lock = lock
        self._expstat = expstat

    def __enter__(self) -> LockIf:
        if self._lock is not None:
            if self._expstat is not None:
                lock_start_ts = time.time()
            self._lock.__enter__()
            if self._expstat is not None:
                self._expstat.add_lock_time(time.time() - lock_start_ts)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._lock is not None:
            self._lock.__exit__(exc_type, exc_val, exc_tb)
        return


class AsyncPodNamespace(PodNamespace):
    def __init__(self, pod_storage: AsyncPodObjectStorage, *args, **kwargs) -> None:
        PodNamespace.__init__(self, *args, **kwargs)
        self._pod_storage = pod_storage
        self._all_locks: Set[threading.Lock] = set()
        self._lock_by_name: Dict[str, threading.Lock] = {}
        self._names_by_saving_thread: Dict[int, Set[str]] = {}

    def __getitem__(self, name: str) -> Object:
        if threading.get_ident() in self._names_by_saving_thread:  # Skip activating name for saving tread.
            return dict.__getitem__(self, name)
        with LockIf(self._lock_by_name.get(name, None), self._pod_storage._expstat):
            return PodNamespace.__getitem__(self, name)

    def __setitem__(self, name: str, obj: Object) -> None:
        if threading.get_ident() in self._names_by_saving_thread:
            return dict.__setitem__(self, name, obj)
        with LockIf(self._lock_by_name.get(name, None), self._pod_storage._expstat):
            return PodNamespace.__setitem__(self, name, obj)

    def __delitem__(self, name: str):
        if threading.get_ident() in self._names_by_saving_thread:
            return dict.__delitem__(self, name)
        with LockIf(self._lock_by_name.get(name, None), self._pod_storage._expstat):
            return PodNamespace.__delitem__(self, name)

    def items(self):
        if threading.get_ident() in self._names_by_saving_thread:
            # Assuming saving threads do not mutate namespace.
            return dict.items(self)

        if self._pod_storage._expstat is not None:
            lock_start_ts = time.time()
        for lock in list(self._all_locks):
            lock.acquire()
        if self._pod_storage._expstat is not None:
            self._pod_storage._expstat.add_lock_time(time.time() - lock_start_ts)
        try:
            return PodNamespace.items(self)
        finally:
            for lock in self._all_locks:
                lock.release()

    def lock(self, names: Set[str]) -> None:
        assert len(names) > 0
        assert all(name not in self._lock_by_name for name in names)
        lock = threading.Lock()
        lock.acquire()
        self._all_locks.add(lock)
        for name in names:
            self._lock_by_name[name] = lock
        self._names_by_saving_thread[threading.get_ident()] = names

    def release(self) -> None:
        names = self._names_by_saving_thread.pop(threading.get_ident())
        lock = self._lock_by_name[next(iter(names))]
        lock.release()
        self._all_locks.discard(lock)
        for name in names:
            del self._lock_by_name[name]

    def _is_saving_thread(self, name) -> bool:
        return name in self._names_by_saving_thread.get(threading.get_ident(), {})


class AsyncPodSaveThread(threading.Thread):
    def __init__(
        self,
        pod_storage: AsyncPodObjectStorage,
        tid: TimeId,
        namespace: AsyncPodNamespace,
        namemap_pid: PodId,
        namemap: Namemap,
        *args,
        **kwargs,
    ):
        threading.Thread.__init__(self, *args, **kwargs)
        self._pod_storage = pod_storage
        self._tid = tid
        self._namespace = namespace
        self._namemap_pid = namemap_pid
        self._namemap = namemap

        self._podspace = {
            pid: dict.__getitem__(self._namespace, name) for name, pid in self._namemap.items() if pid.tid == self._tid
        }
        self._active_names = {name for name, pid in self._namemap.items() if pid.tid == self._tid}

        self._run_barrier = threading.Barrier(2, timeout=5)  # This should be quick (<1 second).

    def run(self):
        # Lock and wait at barrier to continue with the main thread.
        if len(self._active_names) > 0:  # No locking needed if no active name.
            self._namespace.lock(self._active_names)
        self.wait_run_barrier()

        if self._pod_storage._expstat is not None:
            dump_start_ts = time.time()

        # Actual saving.
        with self._pod_storage._pickling.dump_batch({self._namemap_pid: self._namemap, **self._podspace}) as dump_session:
            dump_session.dump(self._namemap_pid, self._namemap)
            for pid, obj in self._podspace.items():
                dump_session.dump(pid, obj)
            if len(self._active_names) > 0:  # No locking needed if no active name.
                self._namespace.release()

        if self._pod_storage._expstat is not None:
            dump_end_ts = time.time()
            self._pod_storage._expstat.add_async_dump(
                nth=self._tid,
                time_s=dump_end_ts - dump_start_ts,
                storage_b=self._pod_storage.estimate_size(),
            )

    def wait_run_barrier(self):
        self._run_barrier.wait()


class AsyncPodObjectStorage(PodObjectStorage):
    NAMEMAP_OID = 0  # Assume no object resides at this address.

    def __init__(self, pickling: PodPickling) -> None:
        PodObjectStorage.__init__(self, pickling)
        self._save_threads: List[threading.Thread] = []

        self._expstat: Optional[ExpStat] = None

    @staticmethod
    def new(pod_dir: Path) -> PodObjectStorage:
        return AsyncPodObjectStorage.new(pod_dir)

    def new_managed_namespace(self, namespace: Namespace = {}) -> Namespace:
        return AsyncPodNamespace(self, namespace)

    def save(self, namespace: Namespace) -> TimeId:
        assert isinstance(namespace, AsyncPodNamespace)

        # Plan for next namemap.
        tid = step_time_id()
        namemap_pid, namemap = self._save_as_namemap(tid, namespace)

        # Save asynchronously in a different thread.
        # Allow multiple threads to run save at a time.
        save_thread = AsyncPodSaveThread(self, tid, namespace, namemap_pid, namemap)
        save_thread.start()
        save_thread.wait_run_barrier()
        self._save_threads.append(save_thread)

        # Set namemap to the planned one.
        namespace.pod_reset_namemap(namemap)
        return namemap_pid.tid

    def join(self) -> None:
        if self._expstat is not None:
            join_start_ts = time.time()
        for save_thread in self._save_threads:
            save_thread.join()
        self._save_threads.clear()
        if self._expstat is not None:
            self._expstat.add_join_time(time.time() - join_start_ts)

    def instrument(self, expstat: Optional[ExpStat]) -> None:
        self._expstat = expstat
