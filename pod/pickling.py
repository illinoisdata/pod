"""
Pickle protocols based on correlated key-value storages.
"""

from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import enum
import io
from collections import defaultdict
from dataclasses import dataclass
from types import CodeType, FunctionType, ModuleType
from typing import Any, Callable, Dict, List, Optional, Set

import lz4.frame as zlib
import matplotlib.figure
import numpy as np
import pandas as pd
import xdelta3
from loguru import logger

from pod.common import Object, ObjectId, PodDependency, PodDependencyMap, PodId, TimeId, next_rank, step_time_id
from pod.feature import __FEATURE__
from pod.memo import MemoId, MemoPageAllocator, StaticPodPicklerMemo, StaticPodUnpicklerMemo
from pod.storage import PodReader, PodStorage, PodWriter

if pod.__pickle__.BASE_PICKLE == "dill":
    import dill as pickle
    from dill import Pickler as BasePickler
    from dill import Unpickler as BaseUnpickler
elif pod.__pickle__.BASE_PICKLE == "cloudpickle":
    from pickle import _Unpickler as BaseUnpickler

    import cloudpickle as pickle
    from cloudpickle import Pickler as BasePickler
else:  # USE_PICKLE
    import pickle
    from pickle import Pickler as BasePickler
    from pickle import Unpickler as BaseUnpickler


try:
    from types import NoneType
except ImportError:
    NoneType = type(None)  # type: ignore


class PodPicklingDumpSession:
    def __enter__(self) -> PodPicklingDumpSession:
        return self

    def dump(self, pid: PodId, obj: Object) -> None:
        raise NotImplementedError("Abstract method")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


class PodPickling:
    def dump(self, obj: Object) -> PodId:
        raise NotImplementedError("Abstract method")

    def dump_batch(self, pods: Dict[PodId, Object]) -> PodPicklingDumpSession:
        raise NotImplementedError("Abstract method")

    def load(self, pid: PodId) -> Object:
        raise NotImplementedError("Abstract method")

    def load_batch(self, pids: Set[PodId]) -> Dict[PodId, Object]:
        raise NotImplementedError("Abstract method")

    def connected_pods(self) -> Dict[PodId, PodId]:
        raise NotImplementedError("Abstract method")

    def estimate_size(self) -> int:
        raise NotImplementedError("Abstract method")


""" Snapshot: pickling object as a whole """


class SnapshotPodPicklingDumpSession(PodPicklingDumpSession):
    def __init__(self, pickling: SnapshotPodPickling) -> None:
        self.pickling = pickling

    def dump(self, pid: PodId, obj: Object) -> None:
        with open(self.pickling.pickle_path(pid), "wb") as f:
            pickle.dump(obj, f)


class SnapshotPodPickling(PodPickling):
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def dump(self, obj: Object) -> PodId:
        tid = step_time_id()
        pid = PodId(tid, id(obj))
        with open(self.pickle_path(pid), "wb") as f:
            pickle.dump(obj, f)
        return pid

    def dump_batch(self, pods: Dict[PodId, Object]) -> PodPicklingDumpSession:
        return SnapshotPodPicklingDumpSession(self)

    def load(self, pid: PodId) -> Object:
        with open(self.pickle_path(pid), "rb") as f:
            return pickle.load(f)

    def load_batch(self, pids: Set[PodId]) -> Dict[PodId, Object]:
        return {pid: self.load(pid) for pid in pids}

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self.root_dir.glob("**/*") if f.is_file())

    def pickle_path(self, pid: PodId) -> Path:
        return self.root_dir / f"{pid.tid}_{pid.oid}.pkl"


""" CompressedSnapshot: pickling object as a whole """


class CompressedSnapshotPodPicklingDumpSession(PodPicklingDumpSession):
    def __init__(self, pickling: CompressedSnapshotPodPickling) -> None:
        self.pickling = pickling

    def dump(self, pid: PodId, obj: Object) -> None:
        with open(self.pickling.pickle_path(pid), "wb") as f:
            f.write(self.pickling.compress(pickle.dumps(obj)))


class CompressedSnapshotPodPickling(PodPickling):
    def __init__(self, root_dir: Path, delta: bool = False) -> None:
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.delta = delta
        self.ref_bytes: Optional[bytes] = None

    def dump(self, obj: Object) -> PodId:
        tid = step_time_id()
        pid = PodId(tid, id(obj))
        with open(self.pickle_path(pid), "wb") as f:
            f.write(self.compress(pickle.dumps(obj)))
        return pid

    def dump_batch(self, pods: Dict[PodId, Object]) -> PodPicklingDumpSession:
        return CompressedSnapshotPodPicklingDumpSession(self)

    def load(self, pid: PodId) -> Object:
        with open(self.pickle_path(pid), "rb") as f:
            return pickle.loads(self.decompress(f.read()))

    def load_batch(self, pids: Set[PodId]) -> Dict[PodId, Object]:
        return {pid: self.load(pid) for pid in pids}

    def write_ref(self, ref_bytes: bytes):
        with open(self.ref_path(), "wb") as f:
            f.write(ref_bytes)

    def read_ref(self):
        with open(self.ref_path(), "rb") as f:
            return f.read()

    def compress(self, obj_bytes: bytes) -> bytes:
        if self.delta:
            if self.ref_bytes is None:
                self.ref_bytes = obj_bytes
                self.write_ref(self.ref_bytes)
            compressed_obj_bytes = xdelta3.encode(self.ref_bytes, obj_bytes)
            logger.debug(f"Delta-compressed {len(obj_bytes)} to {len(compressed_obj_bytes)}")
            return compressed_obj_bytes
        else:
            compressed_obj_bytes = zlib.compress(obj_bytes)
            logger.debug(f"Zlib-compressed {len(obj_bytes)} to {len(compressed_obj_bytes)}")
            return compressed_obj_bytes

    def decompress(self, compressed_obj_bytes: bytes) -> bytes:
        if self.delta:
            return xdelta3.decode(self.read_ref(), compressed_obj_bytes)
        else:
            return zlib.decompress(compressed_obj_bytes)

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self.root_dir.glob("**/*") if f.is_file())

    def pickle_path(self, pid: PodId) -> Path:
        return self.root_dir / f"{pid.tid}_{pid.oid}.zpkl"

    def ref_path(self) -> Path:
        return self.root_dir / "ref.bin"


""" Many: pickling object using many picklings (e.g., exhaustive search) """


class ManyPodPicklingDumpSession(PodPicklingDumpSession):
    def __init__(self, many_pickling: ManyPodPickling, pods: Dict[PodId, Object]) -> None:
        self.many_pickling = many_pickling
        self.pods = pods

        self.pickling_batches = [(pickling.dump_batch(pods), MemoPageAllocator()) for pickling in self.many_pickling.picklings]
        self.original_allocator: Optional[MemoPageAllocator] = None

    def __enter__(self) -> ManyPodPicklingDumpSession:
        self.original_allocator = StaticPodPicklerMemo.PAGE_ALLOCATOR
        for pickling_batch, _ in self.pickling_batches:
            pickling_batch.__enter__()
        return self

    def dump(self, pid: PodId, obj: Object) -> None:
        for pickling_batch, allocator in self.pickling_batches:
            StaticPodPicklerMemo.PAGE_ALLOCATOR = allocator
            pickling_batch.dump(pid, obj)

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        assert self.original_allocator is not None
        StaticPodPicklerMemo.PAGE_ALLOCATOR = self.original_allocator
        for pickling_batch, _ in self.pickling_batches:
            pickling_batch.__exit__(exc_type, exc_val, exc_tb)


class ManyPodPickling(PodPickling):
    def __init__(self, picklings: List[PodPickling]) -> None:
        self.picklings = picklings

    def dump(self, obj: Object) -> PodId:
        tid = step_time_id()
        pid = PodId(tid, id(obj))
        with self.dump_batch({pid: obj}) as session:
            session.dump(pid, obj)
        return pid

    def dump_batch(self, pods: Dict[PodId, Object]) -> PodPicklingDumpSession:
        return ManyPodPicklingDumpSession(self, pods)

    def load(self, pid: PodId) -> Object:
        # Load from the first pickling, because our experiment doesn't cover this part.
        return self.picklings[0].load(pid)

    def load_batch(self, pids: Set[PodId]) -> Dict[PodId, Object]:
        # Load from the first pickling, because our experiment doesn't cover this part.
        return self.picklings[0].load_batch(pids)

    def connected_pods(self) -> Dict[PodId, PodId]:
        # Find from the first pickling, because our experiment doesn't cover this part.
        return self.picklings[0].connected_pods()

    def estimate_size(self) -> int:
        # Minimum supposing we use only that one pickling.
        return min(pickling.estimate_size() for pickling in self.picklings)


""" Pickling objects into pod consistently on object identity """


class PodAction(enum.Enum):
    bundle = "bundle"
    split = "split"
    split_final = "split_final"


PoddingFunction = Callable[[Object, BasePickler], PodAction]
PostPoddingFunction = Callable[[PodDependencyMap], None]


class ManualPodding:
    BUNDLE_TYPES = (
        # Constant/small size.
        float,
        int,
        complex,
        bool,
        tuple,
        NoneType,
    )
    SPLIT_TYPES = (
        # Builtin types.
        str,
        bytes,
        list,
        dict,
        type,
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
        type,
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
    SPLIT_FINAL_AT_DEP = 1

    @staticmethod
    def podding_fn(obj: Object, pickler: BasePickler) -> PodAction:
        if isinstance(obj, ManualPodding.BUNDLE_TYPES):
            return PodAction.bundle

        # Extract object module.
        obj_module = getattr(obj, "__module__", None)
        obj_module = obj_module.split(".")[0] if isinstance(obj_module, str) else None
        is_split = isinstance(obj, ManualPodding.SPLIT_TYPES) or obj_module in ManualPodding.SPLIT_MODULES
        is_split_final = is_split and (
            isinstance(obj, ManualPodding.FINAL_TYPES)
            or obj_module in ManualPodding.FINAL_MODULES
            or pickler.pod_depth >= ManualPodding.SPLIT_FINAL_AT_DEP
        )

        # Decide whether to split.
        if is_split:
            if is_split_final:
                return PodAction.split_final
            return PodAction.split
        else:
            return PodAction.bundle


@dataclass
class StaticPodPicklerContext:
    root_oids: Set[ObjectId]
    seen_oid: Set[ObjectId]
    dependency_map: PodDependencyMap
    memo: StaticPodPicklerMemo
    cached_pod_actions: Dict[ObjectId, PodAction]
    podding_fn: PoddingFunction

    @staticmethod
    def new(root_pods: Dict[PodId, Object]) -> StaticPodPicklerContext:
        root_oids = {id(obj) for pid, obj in root_pods.items()}
        return StaticPodPicklerContext(
            root_oids=root_oids,
            seen_oid=set(),
            dependency_map={},
            memo=StaticPodPicklerMemo(),
            cached_pod_actions={},
            podding_fn=ManualPodding.podding_fn,
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
            root_memo_id=pickler.memo.get(id(obj), (2**32, obj))[0],
            memo_page_offsets=pickler.memo.page_offsets,
        )

    def dumps(self) -> bytes:
        buffer = io.BytesIO()

        # Head contains fixed-size data.
        buffer.write(self.root_memo_id.to_bytes(StaticPodPicklingMetadata.ROOT_MEMO_ID_BYTES, byteorder="big"))
        buffer.write(len(self.memo_page_offsets).to_bytes(StaticPodPicklingMetadata.MEMO_PAGES_BYTES, byteorder="big"))

        # Next segment contains array of data.
        for page_offset in self.memo_page_offsets:
            buffer.write(page_offset.to_bytes(StaticPodPicklingMetadata.MEMO_PAGE_OFFSET_BYTES, byteorder="big"))

        return buffer.getvalue()

    @staticmethod
    def loads(pickled_self: bytes) -> StaticPodPicklingMetadata:
        buffer = io.BytesIO(pickled_self)

        # Read head segment.
        root_memo_id = int.from_bytes(buffer.read(StaticPodPicklingMetadata.ROOT_MEMO_ID_BYTES), byteorder="big")
        memo_pages = int.from_bytes(buffer.read(StaticPodPicklingMetadata.MEMO_PAGES_BYTES), byteorder="big")

        # Now, read next segment with array of data.
        memo_page_offsets = [
            int.from_bytes(buffer.read(StaticPodPicklingMetadata.MEMO_PAGE_OFFSET_BYTES), byteorder="big")
            for _ in range(memo_pages)
        ]

        return StaticPodPicklingMetadata(
            root_memo_id=root_memo_id,
            memo_page_offsets=memo_page_offsets,
        )


# from pod.stats import PodPicklingStat  # stat_staticppick
# pod_pickling_stat = PodPicklingStat()  # stat_staticppick


class BaseStaticPodPickler(BasePickler):
    IMMUTABLE_TYPES = (
        # Primitive immutable types.
        str,
        bytes,
        int,
        float,
        complex,
        bool,
        # Immutable collections.
        tuple,
        frozenset,
        NoneType,
        # Not all class types are immutable but pickle saves them as a global object anyway.
        type,
        FunctionType,
        ModuleType,
        CodeType,
        pd.DataFrame,
    )

    def __init__(self, *args, **kwargs) -> None:
        if pod.__pickle__.BASE_PICKLE == "dill":
            # Always recurse to analyze referred global variables (e.g., in functions).
            kwargs["recurse"] = True
        BasePickler.__init__(self, *args, **kwargs)

    def get_root_dep(self) -> PodDependency:
        raise NotImplementedError("Abstract method")

    def is_immutable(self, obj: Object) -> bool:
        if isinstance(obj, dict):
            return len(set(obj) & {"_mgr", "_typ", "_metadata", "attrs", "_flags"}) == 5
        return isinstance(obj, BaseStaticPodPickler.IMMUTABLE_TYPES)


class FinalPodPickler(BaseStaticPodPickler):
    def __init__(
        self,
        root_obj: Object,
        root_pid: PodId,
        ctx: StaticPodPicklerContext,
        file: io.IOBase,
        pickle_kwargs: Dict[str, Any] = {},
    ) -> None:
        BaseStaticPodPickler.__init__(self, file, **pickle_kwargs)
        self.root_obj = root_obj
        self.root_pid = root_pid
        self.memo = ctx.memo.next_view(root_pid)
        self.root_rank = next_rank()

    def persistent_id(self, obj: Object) -> Optional[ObjectId]:
        if __FEATURE__.is_enabled:
            __FEATURE__.new_pod_oid(self.root_pid, id(obj))
        return None

    def get_root_dep(self) -> PodDependency:
        return PodDependency(
            dep_pids=self.memo.get_dep_pids(),
            rank=self.root_rank,
            meta=bytes(),
            immutable=self.is_immutable(self.root_obj),
        )


class StaticPodPickler(BaseStaticPodPickler):
    MUST_BUNDLE_TYPES = (
        float,
        int,
        complex,
        bool,
        tuple,
        NoneType,
    )

    def __init__(
        self,
        root_obj: Object,
        root_pid: PodId,
        ctx: StaticPodPicklerContext,
        writer: PodWriter,
        pod_depth: int,
        file: io.IOBase,
        pickle_kwargs: Dict[str, Any] = {},
    ) -> None:
        BaseStaticPodPickler.__init__(self, file, **pickle_kwargs)
        self.root_obj = root_obj
        self.root_pid = root_pid
        self.root_dep_pids: Set[PodId] = set()
        self.root_rank = next_rank()
        self.ctx = ctx
        self.writer = writer
        self.pod_depth = pod_depth
        self.file = file
        self.pickle_kwargs = pickle_kwargs

        # Replace memo with virtual view memo.
        self.memo = self.ctx.memo.next_view(root_pid)

    def persistent_id(self, obj: Object) -> Optional[ObjectId]:
        if obj is self.root_obj:
            # Always bundle root object, otherwise infinite recursion.
            if __FEATURE__.is_enabled:
                __FEATURE__.new_pod_oid(self.root_pid, id(obj))
            return None
        if isinstance(obj, StaticPodPickler.MUST_BUNDLE_TYPES):
            # Always bundle these types without checking the action cache.
            # __FEATURE__.new_pod_oid(self.root_pid, id(obj))
            if __FEATURE__.is_enabled:
                __FEATURE__.new_pod_oid(self.root_pid, id(obj))
            return None

        pod_action = self.safe_podding_fn(obj)
        if pod_action == PodAction.bundle:
            if __FEATURE__.is_enabled:
                __FEATURE__.new_pod_oid(self.root_pid, id(obj))
            return None

        # Split and split final.
        oid = id(obj)
        pid = PodId(self.root_pid.tid, oid)
        if oid not in self.ctx.seen_oid:  # Only dump and write this pod once.
            self.ctx.seen_oid.add(oid)
            StaticPodPickler.dump_and_pod_write(
                obj,
                pid,
                self.ctx,
                self.writer,
                is_final=(pod_action == PodAction.split_final),
                pod_depth=self.pod_depth + 1,
                pickle_kwargs=self.pickle_kwargs,
            )
        self.root_dep_pids.add(pid)
        return oid

    def safe_podding_fn(self, obj: Object) -> PodAction:
        # Makes podding_fn safe (consistent action per object + auto-bundle on saved object).
        oid = id(obj)
        if oid not in self.ctx.cached_pod_actions:
            if oid in self.memo:
                # Not yet seen by persistent_id but by memoize, this object has been saved in final pickle.
                self.ctx.cached_pod_actions[oid] = PodAction.bundle
            else:
                self.ctx.cached_pod_actions[oid] = self.ctx.podding_fn(obj, self)
        return self.ctx.cached_pod_actions[oid]

    def get_root_dep(self) -> PodDependency:
        return PodDependency(
            dep_pids=self.root_dep_pids | self.memo.get_dep_pids(),
            rank=self.root_rank,
            meta=bytes(),
            immutable=self.is_immutable(self.root_obj),
        )

    @staticmethod
    def dump_and_pod_write(
        this_obj: Object,
        this_pid: PodId,
        ctx: StaticPodPicklerContext,
        writer: PodWriter,
        is_final: bool = False,
        pod_depth: int = 0,
        pickle_kwargs: Dict[str, Any] = {},
    ) -> None:
        # Now we have seen this object ID.
        ctx.seen_oid.add(this_pid.oid)

        # Pickle this object into a new buffer.
        this_buffer = io.BytesIO()
        this_pickler = (
            FinalPodPickler(this_obj, this_pid, ctx, this_buffer, **pickle_kwargs)
            if is_final
            else StaticPodPickler(this_obj, this_pid, ctx, writer, pod_depth, this_buffer, pickle_kwargs)
        )
        this_pickler.dump(this_obj)
        this_pod_bytes = this_buffer.getvalue()

        # Add in metadata.
        this_dep = this_pickler.get_root_dep()
        this_metadata = StaticPodPicklingMetadata.new(obj=this_obj, pickler=this_pickler)
        this_dep.meta = this_metadata.dumps()

        # Write to pod storage.
        __FEATURE__.new_pod(this_pid, this_pod_bytes)
        writer.write_pod(this_pid, this_pod_bytes)
        ctx.dependency_map[this_pid] = this_dep

        assert (
            this_metadata.root_memo_id != 2**32 or len(ctx.dependency_map[this_pid].dep_pids) == 0
        ), "Missing root memo ID and potentially contains a self-reference"

        # pod_pickling_stat.append(this_pid, this_obj, this_pod_bytes)  # stat_staticppick


@dataclass
class StaticPodUnpicklerContext:
    tid: TimeId
    reader: PodReader
    loaded_objs: Dict[ObjectId, Object]
    unpickler_by_oid: Dict[ObjectId, StaticPodUnpickler]
    memo: StaticPodUnpicklerMemo


class StaticPodUnpickler(BaseUnpickler):
    def __init__(
        self,
        root_oid: ObjectId,
        ctx: StaticPodUnpicklerContext,
        file: io.IOBase,
        *args,
        **kwargs,
    ) -> None:
        BaseUnpickler.__init__(self, file, *args, **kwargs)
        self.root_oid = root_oid
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs
        self.meta = StaticPodPicklingMetadata.loads(self.ctx.reader.read_meta(PodId(self.ctx.tid, root_oid)))

        # Register myself into unpickler by oid.
        self.ctx.unpickler_by_oid[self.root_oid] = self

        # Replace memo with virtual view memo.
        self.memo = self.ctx.memo.view(self.meta.memo_page_offsets)

    def persistent_load(self, oid: ObjectId) -> Object:
        if oid not in self.ctx.loaded_objs:
            self.ctx.loaded_objs[oid] = self.pod_persistent_load(oid)
        return self.ctx.loaded_objs[oid]

    def pod_persistent_load(self, oid: ObjectId) -> Object:
        if oid in self.ctx.unpickler_by_oid:
            try:
                # HACK: Assume Python-based unpickler has created the target object by now.
                return self.ctx.unpickler_by_oid[oid].root_obj()
            except AttributeError:
                raise AttributeError(
                    "Pod under experimment expects Python-based unpickler (e.g., pickle.Unpickler = pickle._Unpickler)."
                )
        with self.ctx.reader.read(PodId(self.ctx.tid, oid)) as obj_io:
            return StaticPodUnpickler(oid, self.ctx, obj_io, *self.args, **self.kwargs).load()

    def root_obj(self) -> Object:
        return self.memo[self.meta.root_memo_id]


class StaticPodPicklingDumpSession(PodPicklingDumpSession):
    def __init__(self, pickling: StaticPodPickling, ctx: StaticPodPicklerContext) -> None:
        self.pickling = pickling
        self.ctx = ctx

        self.writer: Optional[PodWriter] = None

    def __enter__(self) -> StaticPodPicklingDumpSession:
        __FEATURE__.new_dump()
        self.writer = self.pickling.storage.writer().__enter__()
        return self

    def dump(self, pid: PodId, obj: Object) -> None:
        assert self.writer is not None
        if id(obj) not in self.ctx.seen_oid:
            StaticPodPickler.dump_and_pod_write(
                obj,
                pid,
                self.ctx,
                self.writer,
                pickle_kwargs=self.pickling.pickle_kwargs,
            )

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        assert self.writer is not None
        for pod_id, deps in self.ctx.dependency_map.items():
            self.writer.write_dep(pod_id, deps)
            # pod_pickling_stat.fill_dep(pod_id, self.ctx.dependency_map)  # stat_staticppick
        # pod_pickling_stat.summary()  # stat_staticppick

        self.writer.__exit__(exc_type, exc_val, exc_tb)
        self.writer = None

        logger.info(f"num objects= {len(self.ctx.memo.physical_memo)}")  # stat_numobjs

        if self.pickling.post_podding_fn is not None:
            self.pickling.post_podding_fn(self.ctx.dependency_map)


class StaticPodPickling(PodPickling):
    def __init__(
        self,
        storage: PodStorage,
        podding_fn: Optional[PoddingFunction] = None,
        post_podding_fn: Optional[PostPoddingFunction] = None,
        pickle_kwargs: Dict[str, Any] = {},
    ) -> None:
        self.storage = storage
        self.podding_fn = podding_fn
        self.post_podding_fn = post_podding_fn
        self.pickle_kwargs = pickle_kwargs

    def dump(self, obj: Object) -> PodId:
        tid = step_time_id()
        pid = PodId(tid, id(obj))
        with self.dump_batch({pid: obj}) as session:
            session.dump(pid, obj)
        return pid

    def dump_batch(self, pods: Dict[PodId, Object]) -> PodPicklingDumpSession:
        ctx = StaticPodPicklerContext.new(pods)
        if self.podding_fn is not None:
            ctx.podding_fn = self.podding_fn
        return StaticPodPicklingDumpSession(self, ctx)

    def load(self, pid: PodId) -> Object:
        return self.load_batch({pid})[pid]

    def load_batch(self, pids: Set[PodId]) -> Dict[PodId, Object]:
        with self.storage.reader(list(pids)) as reader:
            ctx_by_tid = {
                tid: StaticPodUnpicklerContext(
                    tid=tid,
                    reader=reader,
                    loaded_objs={},
                    unpickler_by_oid={},
                    memo=StaticPodUnpicklerMemo(),
                )
                for tid in set(pid.tid for pid in pids)
            }
            for this_pid in reader.dep_pids_by_rank():
                ctx = ctx_by_tid[this_pid.tid]
                if this_pid.oid not in ctx.loaded_objs:
                    with reader.read(this_pid) as obj_io:
                        ctx.loaded_objs[this_pid.oid] = StaticPodUnpickler(
                            this_pid.oid,
                            ctx,
                            obj_io,
                            **self.pickle_kwargs,
                        ).load()
            return {pid: ctx_by_tid[pid.tid].loaded_objs[pid.oid] for pid in pids}

    def connected_pods(self) -> Dict[PodId, PodId]:
        try:
            return self.storage.connected_pods()
        except NotImplementedError:
            logger.warning(f"{self.storage.__class__.__name__}::connected_pods is not implemented.")
            common_pid = PodId(-1, -1)  # Conservatively say that all pods are connected.
            return defaultdict(default=lambda: common_pid)  # type: ignore

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
        plot_deps(pod_storage.deps)
