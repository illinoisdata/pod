"""
Pickle protocols based on correlated key-value storages.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from queue import Queue
from types import FunctionType
from typing import Any, Dict, Optional, Set

import dill as pickle
from dill import Pickler as BasePickler
from dill import Unpickler as BaseUnpickler

from pod.common import Object, ObjectId, PodId, TimeId, make_pod_id, object_id, step_time_id
from pod.storage import PodReader, PodStorage


# Inherit this class to cache loaded objects (e.g., for shared references).
class PodUnpickler(BaseUnpickler):
    def __init__(self, file: io.IOBase, *args, **kwargs) -> None:
        BaseUnpickler.__init__(self, file, *args, **kwargs)
        self.loaded_objs: dict = {}
        self.args = args
        self.kwargs = kwargs

    def copy_new(self, file: io.IOBase) -> PodUnpickler:
        new_self = self.clone_new(file)
        self.copy_into(new_self)
        return new_self

    # Override this for new type
    def clone_new(self, file: io.IOBase) -> PodUnpickler:
        return PodUnpickler(file, *self.args, **self.kwargs)

    # Override this to transfer new fields.
    def copy_into(self, new_self: PodUnpickler):
        new_self.loaded_objs = self.loaded_objs

    def persistent_load(self, oid: ObjectId) -> Object:
        if oid not in self.loaded_objs:
            self.loaded_objs[oid] = self.pod_persistent_load(oid)
        return self.loaded_objs[oid]

    def pod_persistent_load(self, oid: ObjectId) -> Object:
        raise NotImplementedError("Abstract method")


class PodPickling:
    def dump(self, obj: Object) -> PodId:
        raise NotImplementedError("Abstract method")

    def load(self, pid: PodId) -> Object:
        raise NotImplementedError("Abstract method")

    def estimate_size(self) -> int:
        raise NotImplementedError("Abstract method")


""" Snapshot: pickling object as a whole """


class SnapshotPodPickling(PodPickling):
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def dump(self, obj: Object) -> PodId:
        tid = step_time_id()
        pid = make_pod_id(tid, object_id(obj))
        with open(self.pickle_path(pid), "wb") as f:
            pickle.dump(obj, f)
        return pid

    def load(self, pid: PodId) -> Object:
        with open(self.pickle_path(pid), "rb") as f:
            return pickle.load(f)

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self.root_dir.glob("**/*") if f.is_file())

    def pickle_path(self, pid: PodId) -> Path:
        return self.root_dir / f"{pid.tid}_{pid.oid}.pkl"


""" Pickling one object per pod """


@dataclass
class IndividualPodPicklerContext:
    seen_oid: Set[ObjectId]
    obj_queue: Queue[Object]

    @staticmethod
    def new(obj: Object) -> IndividualPodPicklerContext:
        obj_queue: Queue[Object] = Queue()
        obj_queue.put(obj)
        seen_oid = {object_id(obj)}
        return IndividualPodPicklerContext(
            seen_oid=seen_oid,
            obj_queue=obj_queue,
        )


class IndividualPodPickler(BasePickler):
    BUNDLE_TYPES = (float, int, complex, bool)

    def __init__(
        self,
        root_obj: Object,
        root_pid: PodId,
        ctx: IndividualPodPicklerContext,
        file: io.IOBase,
        *args,
        **kwargs,
    ) -> None:
        BasePickler.__init__(self, file, *args, **kwargs)
        self.root_obj = root_obj
        self.root_pid = root_pid
        self.root_deps: Set[PodId] = set()
        self.ctx = ctx

    def persistent_id(self, obj: Object) -> Optional[ObjectId]:
        if isinstance(self.root_obj, FunctionType):
            # Don't decompose function object into many pods, due to dill's assumption in save_function.
            return None
        if isinstance(obj, IndividualPodPickler.BUNDLE_TYPES):
            # TODO: Check shared reference.
            return None

        oid = object_id(obj)
        pid = make_pod_id(self.root_pid.tid, oid)
        if pid == self.root_pid:
            # Always save root object, otherwise infinite recursion.
            return None
        if oid not in self.ctx.seen_oid:
            self.ctx.seen_oid.add(oid)
            self.ctx.obj_queue.put(obj)
        self.root_deps.add(pid)
        return oid

    def get_root_deps(self) -> Set[PodId]:
        return self.root_deps


class IndividualPodUnpickler(PodUnpickler):
    def __init__(self, tid: TimeId, reader: PodReader, file: io.IOBase, *args, **kwargs) -> None:
        PodUnpickler.__init__(self, file, *args, **kwargs)
        self.tid = tid
        self.reader = reader
        self.args = args
        self.kwargs = kwargs

    def clone_new(self, file: io.IOBase) -> IndividualPodUnpickler:
        return IndividualPodUnpickler(self.tid, self.reader, file, *self.args, **self.kwargs)

    def copy_into(self, new_self: PodUnpickler):
        PodUnpickler.copy_into(self, new_self)
        new_self.tid = self.tid
        new_self.reader = self.reader

    def pod_persistent_load(self, oid: ObjectId) -> Object:
        # TODO: Load in topological order?
        with self.reader.read(make_pod_id(self.tid, oid)) as obj_io:
            return self.copy_new(obj_io).load()

    @staticmethod
    def load_from_reader(tid: TimeId, reader: PodReader, pid: PodId, *args, **kwargs) -> Object:
        with reader.read(pid) as obj_io:
            return IndividualPodUnpickler(
                tid,
                reader,
                obj_io,
                *args,
                **kwargs,
            ).load()


class IndividualPodPickling(PodPickling):
    def __init__(self, storage: PodStorage, pickle_kwargs: Dict[str, Any] = {}) -> None:
        self.storage = storage
        self.pickle_kwargs = pickle_kwargs

    def dump(self, obj: Object) -> PodId:
        tid = step_time_id()
        pid = make_pod_id(tid, object_id(obj))
        ctx = IndividualPodPicklerContext.new(obj)
        dependency_maps: Dict[PodId, Set] = {}
        with self.storage.writer() as writer:
            while not ctx.obj_queue.empty():
                this_obj = ctx.obj_queue.get()
                this_pid = make_pod_id(tid, object_id(this_obj))
                this_buffer = io.BytesIO()
                this_pickler = IndividualPodPickler(
                    this_obj,
                    this_pid,
                    ctx,
                    this_buffer,
                    **self.pickle_kwargs,
                )
                this_pickler.dump(this_obj)
                this_pod_bytes = this_buffer.getvalue()
                writer.write_pod(this_pid, this_pod_bytes)
                dependency_maps[this_pid] = this_pickler.get_root_deps()
            for pod_id, deps in dependency_maps.items():
                writer.write_dep(pod_id, deps)

        return pid

    def load(self, pid: PodId) -> Object:
        with self.storage.reader([pid]) as reader:
            return IndividualPodUnpickler.load_from_reader(
                pid.tid,
                reader,
                pid,
                **self.pickle_kwargs,
            )

    def estimate_size(self) -> int:
        return self.storage.estimate_size()


if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    from pod.common import plot_deps
    from pod.storage import DictPodStorage, FilePodStorage, Neo4jPodStorage, PostgreSQLPodStorage, RedisPodStorage

    # Initialize storage
    # storage_mode = "dict"
    storage_mode = "file"
    # storage_mode = "postgres"
    # storage_mode = "redis"
    # storage_mode = "neo4j"
    pod_storage: Optional[PodStorage] = None
    if storage_mode == "dict":
        pod_storage = DictPodStorage()
    elif storage_mode == "file":
        tmp_dir = Path(tempfile.gettempdir())
        root_dir = tmp_dir / "pod_test"
        print(f"root_dir= {root_dir}")
        pod_storage = FilePodStorage(root_dir)
    elif storage_mode == "postgres":
        pod_storage = PostgreSQLPodStorage("localhost", 5432)
    elif storage_mode == "redis":
        pod_storage = RedisPodStorage("localhost", 6379)
    elif storage_mode == "neo4j":
        pod_storage = Neo4jPodStorage("neo4j://localhost", 7687)
    else:
        raise ValueError(f"Invalid storage_mode= {storage_mode}")

    # Initialize pickling
    pickling_mode = "individual"
    if pickling_mode == "individual":
        pod_pickling = IndividualPodPickling(pod_storage)
    else:
        raise ValueError(f"Invalid pickling_mode= {pickling_mode}")

    # Save namespace.
    shared_buf = [0, 1, 2, 3, 4, 5, 6]
    namespace: dict = {"x": 1, "y": [2, shared_buf], "z": [3, shared_buf]}
    pid_1 = pod_pickling.dump(namespace)
    print(pid_1, pod_pickling.load(pid_1))
    print(f"Storage size: {pod_storage.estimate_size()}, " f"raw pickle size: {len(pickle.dumps(namespace))}")

    # Namespace mutates.
    namespace["y"][0] = 22
    pid_2 = pod_pickling.dump(namespace)
    print(pid_2, pod_pickling.load(pid_2))
    print(f"Storage size: {pod_storage.estimate_size()}, " f"raw pickle size: {len(pickle.dumps(namespace))}")

    # Test mutating shared reference.
    load_namespace = pod_pickling.load(pid_2)
    load_namespace["y"][1].append("new item")
    print(load_namespace)

    # Visualize dependency graph.
    if isinstance(pod_storage, DictPodStorage):
        plot_deps(pod_storage.dep_pids)
