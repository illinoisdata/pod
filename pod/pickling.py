"""
Pickle protocols based on correlated key-value storages.
"""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from queue import Queue
from types import CodeType, FunctionType, ModuleType, NoneType
from typing import Any, Dict, Optional, Set

import dill as pickle
import matplotlib.figure
import numpy as np
import pandas as pd
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
class StaticPodPicklerJob:
    obj: Object
    is_final: bool


@dataclass
class StaticPodPicklerContext:
    seen_oid: Set[ObjectId]
    job_queue: Queue[StaticPodPicklerJob]

    @staticmethod
    def new(obj: Object) -> StaticPodPicklerContext:
        job_queue: Queue[StaticPodPicklerJob] = Queue()
        job_queue.put(StaticPodPicklerJob(obj, is_final=False))
        seen_oid = {object_id(obj)}
        return StaticPodPicklerContext(
            seen_oid=seen_oid,
            job_queue=job_queue,
        )


class StaticPodPickler(BasePickler):
    BUNDLE_TYPES = (
        # Constant/small size.
        float,
        int,
        complex,
        bool,
        # Forbidden due to dill's assumption in save_function.
        tuple,
        dict,
        # Common ignores.
        NoneType,
        type,
    )
    SPLIT_TYPES = (
        # Builtin types.
        str,
        bytes,
        list,
        # Numerical types.
        np.ndarray,
        pd.DataFrame,
        matplotlib.figure.Figure,
        # Nested types.
        FunctionType,
        ModuleType,
        CodeType,
    )
    FINAL_TYPES = (
        # Builtin types.
        str,
        bytes,
        # Numerical types.
        np.ndarray,
        pd.DataFrame,
        matplotlib.figure.Figure,
    )
    SPLIT_MODULES = {
        "sklearn",
    }
    FINAL_MODULES = {
        "sklearn",
    }

    def __init__(
        self,
        root_obj: Object,
        root_pid: PodId,
        ctx: StaticPodPicklerContext,
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
        if isinstance(obj, StaticPodPickler.BUNDLE_TYPES):
            # TODO: Check shared reference.
            return None
        obj_module = getattr(obj, "__module__", None)
        obj_module = obj_module.split(".")[0] if isinstance(obj_module, str) else None
        if not (isinstance(obj, StaticPodPickler.SPLIT_TYPES) or obj_module in StaticPodPickler.SPLIT_MODULES):
            # Split only allowed types and modules.
            # print(type(obj), obj_module)
            return None

        oid = object_id(obj)
        pid = make_pod_id(self.root_pid.tid, oid)
        if pid == self.root_pid:
            # Always save root object, otherwise infinite recursion.
            return None
        if oid not in self.ctx.seen_oid:
            self.ctx.seen_oid.add(oid)
            self.ctx.job_queue.put(
                StaticPodPicklerJob(
                    obj=obj,
                    is_final=(isinstance(obj, StaticPodPickler.FINAL_TYPES) or obj_module in StaticPodPickler.FINAL_MODULES),
                )
            )
        self.root_deps.add(pid)
        return oid

    def get_root_deps(self) -> Set[PodId]:
        return self.root_deps


class StaticPodPicklingMetadata:
    ROOT_MEMO_ID_BYTES = 8

    def __init__(self, root_memo_id: int) -> None:
        self.root_memo_id = root_memo_id  # For self-referential objects.

    @staticmethod
    def new(
        pickler: BasePickler,
        job: StaticPodPicklerJob,
    ) -> StaticPodPicklingMetadata:
        return StaticPodPicklingMetadata(
            root_memo_id=pickler.memo[id(job.obj)][0],
        )

    def write(self, buffer: io.IOBase) -> None:
        buffer.write(self.root_memo_id.to_bytes(StaticPodPicklingMetadata.ROOT_MEMO_ID_BYTES, byteorder="big"))

    @staticmethod
    def read_from(buffer: io.IOBase) -> StaticPodPicklingMetadata:
        # Keep in sync with write.
        orig_pos = buffer.tell()
        buffer.seek(-StaticPodPicklingMetadata.ROOT_MEMO_ID_BYTES, os.SEEK_END)
        root_memo_id = int.from_bytes(buffer.read(StaticPodPicklingMetadata.ROOT_MEMO_ID_BYTES), byteorder="big")
        buffer.seek(orig_pos, os.SEEK_SET)
        return StaticPodPicklingMetadata(
            root_memo_id=root_memo_id,
        )

    def __reduce__(self):
        return self.__class__, (self.obj_bytes, self.root_memo_id)


@dataclass
class StaticPodUnpicklerContext:
    tid: TimeId
    reader: PodReader
    unpickler_by_oid: Dict[ObjectId, StaticPodUnpickler]


class StaticPodUnpickler(PodUnpickler):
    def __init__(self, ctx: StaticPodUnpicklerContext, file: io.IOBase, *args, **kwargs) -> None:
        PodUnpickler.__init__(self, file, *args, **kwargs)
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs
        self.meta = StaticPodPicklingMetadata.read_from(file)

    def clone_new(self, file: io.IOBase) -> StaticPodUnpickler:
        return StaticPodUnpickler(self.ctx, file, *self.args, **self.kwargs)

    def copy_into(self, new_self: PodUnpickler):
        PodUnpickler.copy_into(self, new_self)
        new_self.ctx = self.ctx

    def pod_persistent_load(self, oid: ObjectId) -> Object:
        # TODO: Load in topological order without recursion?
        if oid in self.ctx.unpickler_by_oid:
            try:
                # HACK: Assume Python-based unpickler has created the target object by now.
                return self.ctx.unpickler_by_oid[oid].root_obj()
            except AttributeError:
                raise AttributeError(
                    "Pod under experimment expects Python-based unpickler (e.g., pickle.Unpickler = pickle._Unpickler)."
                )
        with self.ctx.reader.read(make_pod_id(self.ctx.tid, oid)) as obj_io:
            new_self = self.copy_new(obj_io)
            self.ctx.unpickler_by_oid[oid] = new_self
            return new_self.load()

    def root_obj(self) -> Object:
        return self.memo[self.meta.root_memo_id]

    @staticmethod
    def load_from_reader(reader: PodReader, pid: PodId, *args, **kwargs) -> Object:
        with reader.read(pid) as obj_io:
            ctx = StaticPodUnpicklerContext(
                tid=pid.tid,
                reader=reader,
                unpickler_by_oid={},
            )
            unpickler = StaticPodUnpickler(
                ctx,
                obj_io,
                *args,
                **kwargs,
            )
            ctx.unpickler_by_oid[pid.oid] = unpickler
            return unpickler.load()


class StaticPodPickling(PodPickling):
    def __init__(self, storage: PodStorage, pickle_kwargs: Dict[str, Any] = {}) -> None:
        self.storage = storage
        self.pickle_kwargs = pickle_kwargs

    def dump(self, obj: Object) -> PodId:
        tid = step_time_id()
        pid = make_pod_id(tid, object_id(obj))
        ctx = StaticPodPicklerContext.new(obj)
        dependency_maps: Dict[PodId, Set[PodId]] = {}

        # from pod.stats import PodPicklingStat  # stat_staticppick
        # stat = PodPicklingStat()  # stat_staticppick
        with self.storage.writer() as writer:
            while not ctx.job_queue.empty():
                this_job = ctx.job_queue.get()
                this_pid = make_pod_id(tid, object_id(this_job.obj))
                this_buffer = io.BytesIO()
                this_pickler = (
                    BasePickler(this_buffer, **self.pickle_kwargs)
                    if this_job.is_final
                    else StaticPodPickler(
                        this_job.obj,
                        this_pid,
                        ctx,
                        this_buffer,
                        **self.pickle_kwargs,
                    )
                )
                this_pickler.dump(this_job.obj)
                StaticPodPicklingMetadata(root_memo_id=this_pickler.memo[id(this_job.obj)][0]).write(this_buffer)
                this_pod_bytes = this_buffer.getvalue()
                writer.write_pod(this_pid, this_pod_bytes)
                dependency_maps[this_pid] = {} if this_job.is_final else this_pickler.get_root_deps()
                # stat.append(this_pid, this_job.obj, this_pod_bytes)  # stat_staticppick
            for pod_id, deps in dependency_maps.items():
                writer.write_dep(pod_id, deps)
                # stat.fill_dep(pod_id, dependency_maps)  # stat_staticppick
            # stat.summary()  # stat_staticppick

        return pid

    def load(self, pid: PodId) -> Object:
        with self.storage.reader([pid]) as reader:
            return StaticPodUnpickler.load_from_reader(
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
    from pod.storage import (
        DictPodStorage,
        FilePodStorage,
        MongoPodStorage,
        Neo4jPodStorage,
        PostgreSQLPodStorage,
        RedisPodStorage,
    )

    # Initialize storage
    # storage_mode = "dict"
    # storage_mode = "file"
    # storage_mode = "postgres"
    # storage_mode = "redis"
    # storage_mode = "neo4j"
    storage_mode = "mongo"
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
        pod_storage = Neo4jPodStorage("neo4j://localhost", 7687, "pod_neo4j")
    elif storage_mode == "mongo":
        pod_storage = MongoPodStorage("localhost", 27017)
    else:
        raise ValueError(f"Invalid storage_mode= {storage_mode}")

    # Initialize pickling
    pickling_mode = "statis"
    if pickling_mode == "statis":
        pod_pickling = StaticPodPickling(pod_storage)
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
