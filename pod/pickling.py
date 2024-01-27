"""
Pickle protocols based on correlated key-value storages.
"""

from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import io
import os
from bisect import bisect_right
from dataclasses import dataclass
from types import CodeType, FunctionType, ModuleType, NoneType
from typing import Any, Dict, List, Optional, Set, Tuple

import dill as pickle
import matplotlib.figure
import numpy as np
import pandas as pd
from dill import Pickler as BasePickler
from dill import Unpickler as BaseUnpickler

from pod.common import Object, ObjectId, PodId, TimeId, make_pod_id, object_id, step_time_id
from pod.storage import PodReader, PodStorage, PodWriter


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


""" Pickling objects into pod using type-static heuristic """


MemoId = int


class StaticPodPicklerMemo:
    VIRTUAL_OFFSET = 2**31  # [VIRTUAL_OFFSET, 2 ** 32) virtually points to global memo indices.
    PAGE_SIZE = 2**10  # Number of memo objects per page.

    def __init__(self) -> None:
        self.physical_memo: Dict[ObjectId, Tuple[MemoId, Object]] = {}
        self.page_pid: Dict[MemoId, PodId] = {}
        self.latest_page_offset: MemoId = 0

    def next_page_offset(self, pid: PodId) -> MemoId:  # Next ID.
        page_offset = self.latest_page_offset
        self.latest_page_offset += StaticPodPicklerMemo.PAGE_SIZE
        self.page_pid[page_offset] = pid
        assert page_offset < StaticPodPicklerMemo.VIRTUAL_OFFSET
        return page_offset

    def __setitem__(self, obj_id: ObjectId, val: Tuple[MemoId, Object]) -> None:
        self.physical_memo[obj_id] = val

    def __getitem__(self, obj_id: ObjectId) -> Tuple[MemoId, Object, PodId]:
        memo_id, obj = self.physical_memo[obj_id]
        return memo_id, obj, self.page_pid[memo_id - memo_id % StaticPodPicklerMemo.PAGE_SIZE]

    def __contains__(self, obj_id: ObjectId) -> bool:
        return obj_id in self.physical_memo

    def clear(self) -> None:
        self.physical_memo.clear()
        self.page_pid.clear()
        self.latest_page_offset = 0

    def next_view(self, pid: PodId) -> StaticPodPicklerMemoView:
        return StaticPodPicklerMemoView(self, pid)


class StaticPodPicklerMemoView:
    def __init__(self, memo: StaticPodPicklerMemo, pid: PodId) -> None:
        self.memo: StaticPodPicklerMemo = memo
        self.pid = pid
        self.page_offsets: List[MemoId] = [self.memo.next_page_offset(self.pid)]
        self.next_id = 0
        self.dep_pids: Set[PodId] = set()

    def __len__(self) -> MemoId:  # Next ID
        return self.next_id

    def __setitem__(self, obj_id: ObjectId, val: Tuple[MemoId, Object]) -> None:
        assert val[0] == self.next_id
        page_idx = val[0] // StaticPodPicklerMemo.PAGE_SIZE
        local_offset = val[0] % StaticPodPicklerMemo.PAGE_SIZE
        if page_idx >= len(self.page_offsets):
            self.page_offsets.append(self.memo.next_page_offset(self.pid))
        self.memo[obj_id] = (local_offset + self.page_offsets[page_idx], val[1])
        self.next_id += 1

    def __getitem__(self, obj_id: ObjectId) -> Tuple[MemoId, Object]:
        memo_id, obj, dep_pid = self.memo[obj_id]
        page_idx = bisect_right(self.page_offsets, memo_id) - 1
        if page_idx >= 0 and memo_id < self.page_offsets[page_idx] + StaticPodPicklerMemo.PAGE_SIZE:
            # Implicit: self.page_offsets[page_idx] <= memo_id.
            assert self.next_id > 0
            memo_id = (memo_id - self.page_offsets[page_idx]) + page_idx * StaticPodPicklerMemo.PAGE_SIZE
        else:
            memo_id += StaticPodPicklerMemo.VIRTUAL_OFFSET
            self.dep_pids.add(dep_pid)
        return (memo_id, obj)

    def __contains__(self, obj_id: ObjectId) -> bool:
        return obj_id in self.memo

    def get(self, obj_id: ObjectId) -> Optional[Tuple[MemoId, Object]]:
        return self[obj_id] if obj_id in self.memo else None

    def get_dep_pids(self) -> Set[PodId]:
        return self.dep_pids

    # def clear(self) -> None:
    #     raise NotImplementedError("Not yet implemented")


class StaticPodUnpicklerMemo:
    def __init__(self) -> None:
        self.physical_memo: Dict[MemoId, Object] = {}

    def __setitem__(self, memo_id: MemoId, obj: Object) -> None:
        self.physical_memo[memo_id] = obj

    def __getitem__(self, memo_id: ObjectId) -> Object:
        return self.physical_memo[memo_id]

    def view(self, page_offsets: List[MemoId]) -> StaticPodUnpicklerMemoView:
        return StaticPodUnpicklerMemoView(self, page_offsets)


class StaticPodUnpicklerMemoView:
    def __init__(self, memo: StaticPodUnpicklerMemo, page_offsets: List[MemoId]) -> None:
        self.memo: StaticPodUnpicklerMemo = memo
        self.page_offsets = page_offsets
        self.next_id = 0

    def __len__(self) -> MemoId:  # Next ID
        return self.next_id

    def __setitem__(self, memo_id: MemoId, obj: Object) -> None:
        assert memo_id == self.next_id
        page_idx = memo_id // StaticPodPicklerMemo.PAGE_SIZE
        memo_id += self.page_offsets[page_idx]
        self.memo[memo_id] = obj
        self.next_id += 1

    def __getitem__(self, memo_id: ObjectId) -> Object:
        if memo_id < StaticPodPicklerMemo.VIRTUAL_OFFSET:
            page_idx = memo_id // StaticPodPicklerMemo.PAGE_SIZE
            memo_id += self.page_offsets[page_idx]
        else:
            memo_id -= StaticPodPicklerMemo.VIRTUAL_OFFSET
        return self.memo[memo_id]


@dataclass
class StaticPodPicklerJob:
    obj: Object
    is_final: bool


@dataclass
class StaticPodPicklerContext:
    seen_oid: Set[ObjectId]
    dependency_maps: Dict[PodId, Set[PodId]]
    memo: StaticPodPicklerMemo

    @staticmethod
    def new(obj: Object) -> StaticPodPicklerContext:
        seen_oid = {object_id(obj)}
        return StaticPodPicklerContext(
            seen_oid=seen_oid,
            dependency_maps={},
            memo=StaticPodPicklerMemo(),
        )


class StaticPodPicklingMetadata:
    ROOT_MEMO_ID_BYTES = 8
    MEMO_PAGES_BYTES = 4
    MEMO_PAGE_OFFSET_BYTES = 4

    def __init__(self, root_memo_id: int, memo_page_offsets: List[MemoId]) -> None:
        self.root_memo_id = root_memo_id  # For handling self-referential objects.
        self.memo_page_offsets = memo_page_offsets  # For retrieving virtual memo pages.

    @staticmethod
    def new(
        obj: Object,
        pickler: BasePickler,
    ) -> StaticPodPicklingMetadata:
        return StaticPodPicklingMetadata(
            root_memo_id=pickler.memo[id(obj)][0],
            memo_page_offsets=pickler.memo.page_offsets,
        )

    def write(self, buffer: io.IOBase) -> None:
        # Pre-tail segment contains array of data.
        for page_offset in self.memo_page_offsets:
            buffer.write(page_offset.to_bytes(StaticPodPicklingMetadata.MEMO_PAGE_OFFSET_BYTES, byteorder="big"))

        # Tail contains fixed-size data.
        buffer.write(self.root_memo_id.to_bytes(StaticPodPicklingMetadata.ROOT_MEMO_ID_BYTES, byteorder="big"))
        buffer.write(len(self.memo_page_offsets).to_bytes(StaticPodPicklingMetadata.MEMO_PAGES_BYTES, byteorder="big"))

    @staticmethod
    def read_from(buffer: io.IOBase) -> StaticPodPicklingMetadata:
        # Save original position to rewind to.
        orig_pos = buffer.tell()

        # Read tail segment.
        tail_bytes = StaticPodPicklingMetadata.ROOT_MEMO_ID_BYTES + StaticPodPicklingMetadata.MEMO_PAGES_BYTES
        buffer.seek(-tail_bytes, os.SEEK_END)
        root_memo_id = int.from_bytes(buffer.read(StaticPodPicklingMetadata.ROOT_MEMO_ID_BYTES), byteorder="big")
        memo_pages = int.from_bytes(buffer.read(StaticPodPicklingMetadata.MEMO_PAGES_BYTES), byteorder="big")

        # Now, read pre-tail segment
        pretail_bytes = tail_bytes + memo_pages * StaticPodPicklingMetadata.MEMO_PAGE_OFFSET_BYTES
        buffer.seek(-pretail_bytes, os.SEEK_END)
        memo_page_offsets = [
            int.from_bytes(buffer.read(StaticPodPicklingMetadata.MEMO_PAGE_OFFSET_BYTES), byteorder="big")
            for _ in range(memo_pages)
        ]

        # Rewind back to original position.
        buffer.seek(orig_pos, os.SEEK_SET)
        return StaticPodPicklingMetadata(
            root_memo_id=root_memo_id,
            memo_page_offsets=memo_page_offsets,
        )


# from pod.stats import PodPicklingStat  # stat_staticppick
# pod_pickling_stat = PodPicklingStat()  # stat_staticppick


class BaseStaticPodPickler(BasePickler):
    def __init__(self, *args, **kwargs) -> None:
        BasePickler.__init__(self, *args, **kwargs)

    def get_root_deps(self) -> Set[PodId]:
        raise NotImplementedError("Abstract method")


class FinalPodPickler(BaseStaticPodPickler):
    def __init__(
        self,
        root_pid: PodId,
        ctx: StaticPodPicklerContext,
        file: io.IOBase,
        pickle_kwargs: Dict[str, Any] = {},
    ) -> None:
        BaseStaticPodPickler.__init__(self, file, **pickle_kwargs)
        self.memo = ctx.memo.next_view(root_pid)

    def get_root_deps(self) -> Set[PodId]:
        return self.memo.get_dep_pids()


class StaticPodPickler(BaseStaticPodPickler):
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
        writer: PodWriter,
        file: io.IOBase,
        pickle_kwargs: Dict[str, Any] = {},
    ) -> None:
        BaseStaticPodPickler.__init__(self, file, **pickle_kwargs)
        self.root_obj = root_obj
        self.root_pid = root_pid
        self.root_deps: Set[PodId] = set()
        self.ctx = ctx
        self.writer = writer
        self.pickle_kwargs = pickle_kwargs

        # Replace memo with virtual view memo.
        self.memo = self.ctx.memo.next_view(root_pid)

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
            StaticPodPickler.dump_and_pod_write(obj, pid, self.ctx, self.writer, self.pickle_kwargs)
        self.root_deps.add(pid)
        return oid

    def get_root_deps(self) -> Set[PodId]:
        return self.root_deps | self.memo.get_dep_pids()

    @staticmethod
    def dump_and_pod_write(
        this_obj: Object,
        this_pid: PodId,
        ctx: StaticPodPicklerContext,
        writer: PodWriter,
        pickle_kwargs: Dict[str, Any] = {},
    ) -> None:
        # Determine if this object is a final type (prefer base pickler).
        this_obj_module_str = getattr(this_obj, "__module__", None)
        this_obj_module = this_obj_module_str.split(".")[0] if isinstance(this_obj_module_str, str) else None
        is_final = isinstance(this_obj, StaticPodPickler.FINAL_TYPES) or this_obj_module in StaticPodPickler.FINAL_MODULES

        # Pickle this object into a new buffer.
        this_buffer = io.BytesIO()
        this_pickler = (
            FinalPodPickler(this_pid, ctx, this_buffer, **pickle_kwargs)
            if is_final
            else StaticPodPickler(this_obj, this_pid, ctx, writer, this_buffer, pickle_kwargs)
        )
        this_pickler.dump(this_obj)
        StaticPodPicklingMetadata.new(obj=this_obj, pickler=this_pickler).write(this_buffer)
        this_pod_bytes = this_buffer.getvalue()

        # Write to pod storage.
        writer.write_pod(this_pid, this_pod_bytes)
        ctx.dependency_maps[this_pid] = this_pickler.get_root_deps()

        # pod_pickling_stat.append(this_pid, this_obj, this_pod_bytes)  # stat_staticppick


@dataclass
class StaticPodUnpicklerContext:
    tid: TimeId
    reader: PodReader
    unpickler_by_oid: Dict[ObjectId, StaticPodUnpickler]
    memo: StaticPodUnpicklerMemo


class StaticPodUnpickler(PodUnpickler):
    def __init__(self, ctx: StaticPodUnpicklerContext, file: io.IOBase, *args, **kwargs) -> None:
        PodUnpickler.__init__(self, file, *args, **kwargs)
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs
        self.meta = StaticPodPicklingMetadata.read_from(file)

        # Replace memo with virtual view memo.
        self.memo = self.ctx.memo.view(self.meta.memo_page_offsets)

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
                memo=StaticPodUnpicklerMemo(),
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

        with self.storage.writer() as writer:
            StaticPodPickler.dump_and_pod_write(obj, pid, ctx, writer, self.pickle_kwargs)
            for pod_id, deps in ctx.dependency_maps.items():
                writer.write_dep(pod_id, deps)
                # pod_pickling_stat.fill_dep(pod_id, ctx.dependency_maps)  # stat_staticppick
            # pod_pickling_stat.summary()  # stat_staticppick

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
    storage_mode = "file"
    # storage_mode = "postgres"
    # storage_mode = "redis"
    # storage_mode = "neo4j"
    # storage_mode = "mongo"
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
    pickling_mode = "static"
    if pickling_mode == "static":
        pod_pickling = StaticPodPickling(pod_storage)
    else:
        raise ValueError(f"Invalid pickling_mode= {pickling_mode}")

    # Save namespace.
    shared_buf = [0, 1, 2, 3, 4, 5, 6]
    shared_dict = {7: 8, 9: 10}
    namespace: dict = {"x": 1, "y": [2, shared_buf, shared_dict], "z": [3, shared_buf, shared_dict]}
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
    load_namespace["y"][2][11] = "new kv"
    print(load_namespace)

    # Visualize dependency graph.
    if isinstance(pod_storage, DictPodStorage):
        plot_deps(pod_storage.dep_pids)
