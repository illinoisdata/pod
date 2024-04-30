"""
Pod storage interface.
"""

from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import shelve
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import cloudpickle
import dill
import transaction as ZODB_transaction
import ZODB
import ZODB.FileStorage
from loguru import logger

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
            namespace = {name: namespace[name] for name in nameset if name in namespace}
        return namespace

    def estimate_size(self) -> int:
        return self._pickling.estimate_size()


""" Dill namespace storage """


class DillObjectStorage(ObjectStorage):
    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir
        self._root_dir.mkdir(parents=True, exist_ok=True)

    def save(self, namespace: Namespace) -> TimeId:
        tid = step_time_id()
        with open(self.pickle_path(tid), "wb") as f:
            dill.dump(namespace, f)
        return tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        with open(self.pickle_path(tid), "rb") as f:
            namespace = dill.load(f)
        if nameset is not None:
            namespace = {name: namespace[name] for name in nameset if name in namespace}
        return namespace

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self._root_dir.glob("**/*") if f.is_file())

    def pickle_path(self, tid: TimeId) -> Path:
        return self._root_dir / f"t{tid}.pkl"


""" Cloudpickle namespace storage """


class CloudpickleObjectStorage(ObjectStorage):
    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir
        self._root_dir.mkdir(parents=True, exist_ok=True)

    def save(self, namespace: Namespace) -> TimeId:
        tid = step_time_id()
        with open(self.pickle_path(tid), "wb") as f:
            cloudpickle.dump(namespace, f)
        return tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        with open(self.pickle_path(tid), "rb") as f:
            namespace = cloudpickle.load(f)
        if nameset is not None:
            namespace = {name: namespace[name] for name in nameset if name in namespace}
        return namespace

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self._root_dir.glob("**/*") if f.is_file())

    def pickle_path(self, tid: TimeId) -> Path:
        return self._root_dir / f"t{tid}.pkl"


""" Shelve namespace storage """


class ShelveObjectStorage(ObjectStorage):
    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir
        self._root_dir.mkdir(parents=True, exist_ok=True)

    def save(self, namespace: Namespace) -> TimeId:
        tid = step_time_id()
        with shelve.open(self.db_path()) as db:
            db[f"{tid}"] = namespace.keys()
            for name, obj in namespace.items():
                db[f"{tid}:{name}"] = obj
        return tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        with shelve.open(self.db_path()) as db:
            if nameset is None:
                nameset = set(db[f"{tid}"])
            return {name: db[f"{tid}:{name}"] for name in nameset if f"{tid}:{name}" in db}

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self._root_dir.glob("**/*") if f.is_file())

    def db_path(self) -> str:
        import dbm.dumb

        db_path = str(self._root_dir / "shelve")
        db = dbm.dumb.open(db_path, "c")  # Initialize dbm (ndbm does not work)
        db.close()
        return db_path


# Not importable: no longer maintain.

# """ Chest namespace storage """


# class ChestObjectStorage(ObjectStorage):
#     def __init__(self, root_dir: Path) -> None:
#         self._root_dir = root_dir
#         self._root_dir.mkdir(parents=True, exist_ok=True)

#     def save(self, namespace: Namespace) -> TimeId:
#         tid = step_time_id()
#         db = Chest(path=self.db_path(tid), dump=podpickle.dump, load=podpickle.load)
#         for name, obj in namespace.items():
#             db[name] = obj
#         db.flush()
#         return tid

#     def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
#         db = Chest(path=self.db_path(tid), dump=podpickle.dump, load=podpickle.load)
#         if nameset is None:
#             nameset = set(db.keys())
#         return {name: db[name] for name in nameset if name in db}

#     def estimate_size(self) -> int:
#         return sum(f.stat().st_size for f in self._root_dir.glob("**/*") if f.is_file())

#     def db_path(self, tid: TimeId) -> str:
#         return str(self._root_dir / f"t{tid}")


""" ZODB namespace storage """


class ZODBObjectStorage(ObjectStorage):
    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir
        self._root_dir.mkdir(parents=True, exist_ok=True)
        self._storage = ZODB.FileStorage.FileStorage(self.db_path())
        self._db = ZODB.DB(self._storage, large_record_size=1e12)
        self._txn = ZODB_transaction.TransactionManager()
        self._db.setHistoricalTimeout(86400)  # 1 day

        # HACK: Should be written to storage.
        self._tid_p_serial: Dict[int, bytes] = {}

    def save(self, namespace: Namespace) -> TimeId:
        tid = step_time_id()
        connection = self._db.open(transaction_manager=self._txn)
        root = connection.root()
        for name, obj in namespace.items():
            root[name] = obj
        self._txn.commit()
        self._tid_p_serial[tid] = root._p_serial
        connection.close()
        return tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        p_serial = self._tid_p_serial[tid]
        db = ZODB.DB(self._storage, large_record_size=1e12)  # New db to clear cache for benchmark.
        connection = db.open(transaction_manager=self._txn, at=p_serial)
        root = connection.root()
        if nameset is None:
            nameset = set(root.keys())
        loaded_namespace = {name: root[name] for name in nameset if name in root}
        connection.close()
        return loaded_namespace

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self._root_dir.glob("**/*") if f.is_file())

    def db_path(self) -> str:
        return str(self._root_dir / "zodb.fs")


class ZODBSplitObjectStorage(ObjectStorage):
    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir
        self._root_dir.mkdir(parents=True, exist_ok=True)

    def save(self, namespace: Namespace) -> TimeId:
        tid = step_time_id()
        storage = ZODB.FileStorage.FileStorage(self.db_path(tid))
        db = ZODB.DB(storage, large_record_size=1e12)
        connection = db.open()
        root = connection.root()
        for name, obj in namespace.items():
            root[name] = obj
        ZODB_transaction.commit()
        connection.close()
        return tid

    def load(self, tid: TimeId, nameset: Optional[Set[str]] = None) -> Namespace:
        storage = ZODB.FileStorage.FileStorage(self.db_path(tid))
        db = ZODB.DB(storage, large_record_size=1e12)
        connection = db.open()
        root = connection.root()
        if nameset is None:
            nameset = set(root.keys())
        loaded_namespace = {name: root[name] for name in nameset if name in root}
        connection.close()
        return loaded_namespace

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self._root_dir.glob("**/*") if f.is_file())

    def db_path(self, tid: TimeId) -> str:
        return str(self._root_dir / f"zodb_{tid}.fs")


""" Pod namespace storage """


# TODO: Filter read-only variables in function scope dict better.
class PodNamespace(dict):
    def __init__(self, *args, **kwargs) -> None:
        dict.__init__(self, *args, **kwargs)
        self.active_names: Set[str] = set(self.keys())
        self.namemap: Namemap = {}
        self.managed: bool = True

    def __getitem__(self, name: str) -> Object:
        if self.managed:
            self.active_names.add(name)
        return dict.__getitem__(self, name)

    def __setitem__(self, name: str, obj: Object) -> None:
        if self.managed:
            self.active_names.add(name)
        return dict.__setitem__(self, name, obj)

    def __delitem__(self, name: str):
        if self.managed:
            self.active_names.discard(name)
        return dict.__delitem__(self, name)

    def items(self):
        if self.managed:
            self.active_names = set(self.keys())  # TODO: Use enum for this.
        return dict.items(self)

    def set_managed(self, managed: bool):
        self.managed = managed

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

    def __init__(self, pickling: PodPickling, active_filter: bool = True) -> None:
        self._pickling = pickling
        self._active_filter = active_filter
        self._name_rank: Dict[str, int] = {}

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

        if self._active_filter and isinstance(namespace, PodNamespace):
            active_names = self._connected_active_names(tid, namespace)
            logger.warning(f"{active_names=}")
            prev_namemap = namespace.pod_namemap()
            active_namemap = {
                name: PodId(tid, id(dict.__getitem__(namespace, name))) for name in namespace.keys() if name in active_names
            }
            inactive_namemap = {name: prev_namemap[name] for name in namespace.keys() if name not in active_names}
            namemap = {**active_namemap, **inactive_namemap}  # Should contain exactly all names in namespace.
        else:
            namemap = {name: PodId(tid, id(obj)) for name, obj in dict.items(namespace)}
        for name in namemap.keys():
            if name not in self._name_rank:
                self._name_rank[name] = len(self._name_rank)
        return namemap_pid, namemap

    def _load_namemap(self, tid: TimeId) -> Namemap:
        return self._pickling.load(PodId(tid, PodObjectStorage.NAMEMAP_OID))

    def _save_objects(self, tid: TimeId, namespace: Namespace, namemap_pid: PodId, namemap: Namemap) -> None:
        new_namepids = sorted(
            [(name, pid) for name, pid in namemap.items() if pid.tid == tid],
            key=lambda npid: self._name_rank[npid[0]],
        )
        podspace = {pid: dict.__getitem__(namespace, name) for name, pid in new_namepids}
        with self._pickling.dump_batch({namemap_pid: namemap_pid, **podspace}) as dump_session:
            dump_session.dump(namemap_pid, namemap)
            for name, pid in new_namepids:  # This save objects in alphabetical order.
                obj = podspace[pid]
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
    def __init__(self, lock: threading.Lock, pred: bool, expstat: Optional[ExpStat]) -> None:
        self._lock = lock
        self._pred = pred
        self._expstat = expstat

    def __enter__(self) -> LockIf:
        if self._pred:
            if self._expstat is not None:
                lock_start_ts = time.time()
            self._lock.__enter__()
            if self._expstat is not None:
                self._expstat.add_lock_time(time.time() - lock_start_ts)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._pred:
            self._lock.__exit__(exc_type, exc_val, exc_tb)
        return


class AsyncPodNamespace(PodNamespace):
    def __init__(self, pod_storage: AsyncPodObjectStorage, *args, **kwargs) -> None:
        PodNamespace.__init__(self, *args, **kwargs)
        self._pod_storage = pod_storage
        self._sync_lock = threading.Lock()
        self._namespace_lock = threading.Lock()
        self._locked_names: Set[str] = set()
        self._saving_threads: Set[int] = set()

    def __getitem__(self, name: str) -> Object:
        if threading.get_ident() in self._saving_threads or not self.managed:  # Skip activating name for saving tread.
            return dict.__getitem__(self, name)
        with LockIf(self._namespace_lock, self.is_locked_name(name), self._pod_storage._expstat):
            return PodNamespace.__getitem__(self, name)

    def __setitem__(self, name: str, obj: Object) -> None:
        if threading.get_ident() in self._saving_threads or not self.managed:
            return dict.__setitem__(self, name, obj)
        with LockIf(self._namespace_lock, self.is_locked_name(name), self._pod_storage._expstat):
            return PodNamespace.__setitem__(self, name, obj)

    def __delitem__(self, name: str):
        if threading.get_ident() in self._saving_threads or not self.managed:
            return dict.__delitem__(self, name)
        with LockIf(self._namespace_lock, self.is_locked_name(name), self._pod_storage._expstat):
            return PodNamespace.__delitem__(self, name)

    def items(self):
        if threading.get_ident() in self._saving_threads or not self.managed:
            return dict.items(self)
        with LockIf(self._namespace_lock, len(self._locked_names) > 0, self._pod_storage._expstat):
            return PodNamespace.items(self)

    def lock(self, names: Set[str]) -> None:
        with self._sync_lock:
            assert self._namespace_lock.acquire(blocking=False)
            self._locked_names = names
            self._saving_threads.add(threading.get_ident())

    def release(self) -> None:
        with self._sync_lock:
            self._namespace_lock.release()
            self._locked_names.clear()
            self._saving_threads.discard(threading.get_ident())

    def is_locked_name(self, name: str) -> bool:
        return len(self._locked_names) > 0 and (self._pod_storage._always_lock_all or name in self._locked_names)


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
        self._active_name_pids = sorted(
            [(name, pid) for name, pid in self._namemap.items() if pid.tid == self._tid],
            key=lambda npid: self._pod_storage._name_rank[npid[0]],
        )

        self._run_barrier = threading.Barrier(2, timeout=5)  # This should be quick (<1 second).

    def run(self):
        # Lock and wait at barrier to continue with the main thread.
        self._namespace.lock(self._active_names)
        self.wait_run_barrier()

        if self._pod_storage._expstat is not None:
            dump_start_ts = time.time()

        # Actual saving.
        with self._pod_storage._pickling.dump_batch({self._namemap_pid: self._namemap, **self._podspace}) as dump_session:
            dump_session.dump(self._namemap_pid, self._namemap)
            for name, pid in self._active_name_pids:
                obj = self._podspace[pid]
                dump_session.dump(pid, obj)
            self._namespace.release()

        if self._pod_storage._expstat is not None:
            dump_end_ts = time.time()
            self._pod_storage._save_count += 1
            self._pod_storage._expstat.add_async_dump(
                nth=self._pod_storage._save_count,
                time_s=dump_end_ts - dump_start_ts,
                storage_b=self._pod_storage.estimate_size(),
            )

    def wait_run_barrier(self):
        self._run_barrier.wait()


class AsyncPodObjectStorage(PodObjectStorage):
    NAMEMAP_OID = 0  # Assume no object resides at this address.

    def __init__(
        self,
        pickling: PodPickling,
        active_filter: bool = True,
        always_lock_all: bool = False,
    ) -> None:
        PodObjectStorage.__init__(self, pickling, active_filter)
        self._always_lock_all = always_lock_all
        self._running_save: Optional[threading.Thread] = None

        self._expstat: Optional[ExpStat] = None
        self._save_count: int = 0

    @staticmethod
    def new(pod_dir: Path) -> PodObjectStorage:
        return AsyncPodObjectStorage.new(pod_dir)

    def new_managed_namespace(self, namespace: Namespace = {}) -> Namespace:
        return AsyncPodNamespace(self, namespace)

    def save(self, namespace: Namespace) -> TimeId:
        assert isinstance(namespace, AsyncPodNamespace)

        # Join existing saves first.
        self.join()

        # Plan for next namemap.
        tid = step_time_id()
        namemap_pid, namemap = self._save_as_namemap(tid, namespace)

        # Save asynchronously in a different thread.
        self._running_save = AsyncPodSaveThread(self, tid, namespace, namemap_pid, namemap)
        self._running_save.start()
        self._running_save.wait_run_barrier()

        # Set namemap to the planned one.
        namespace.pod_reset_namemap(namemap)
        return namemap_pid.tid

    def join(self) -> None:
        if self._running_save is not None:
            if self._expstat is not None:
                join_start_ts = time.time()
            self._running_save.join()
            if self._expstat is not None:
                self._expstat.add_join_time(time.time() - join_start_ts)

    def instrument(self, expstat: Optional[ExpStat]) -> None:
        self._expstat = expstat
