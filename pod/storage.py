"""
Key-value storages with correlated/poset reads
"""

from __future__ import annotations

import io
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set

from dataclasses_json import dataclass_json

from pod.common import PodId


class PodWriter:
    def __enter__(self) -> PodWriter:
        return self  # Optional: Allocate resources.

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        raise NotImplementedError("Abstract method")

    def write_dep(
        self,
        pod_id: PodId,
        dep_pids: Set[PodId],  # List of pids this pod depends on.
    ) -> None:
        raise NotImplementedError("Abstract method")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass  # Optional: Commit and tear down resources.


class PodReader:
    def __enter__(self) -> PodReader:
        return self  # Optional: Allocate resources.

    def read(self, pod_id: PodId) -> io.IOBase:
        raise NotImplementedError("Abstract method")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass  # Optional: Commit and tear down resources.


class PodStorage:
    def writer(self) -> PodWriter:
        raise NotImplementedError("Abstract method")

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        raise NotImplementedError("Abstract method")

    def estimate_size(self) -> int:
        raise NotImplementedError("Abstract method")


""" Dictionary-based storage (ephemeral, for experimental uses) """


class DictPodStorageWriter(PodWriter):
    def __init__(self, storage: DictPodStorage) -> None:
        self.storage = storage

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        self.storage.pod_bytes[pod_id] = pod_bytes

    def write_dep(
        self,
        pod_id: PodId,
        dep_pids: Set[PodId],  # List of pids this pod depends on.
    ) -> None:
        self.storage.dep_pids[pod_id] = dep_pids


class DictPodStorageReader(PodReader):
    def __init__(self, storage: DictPodStorage) -> None:
        self.storage = storage

    def read(self, pod_id: PodId) -> io.IOBase:
        return io.BytesIO(self.storage.pod_bytes[pod_id])


class DictPodStorage(PodStorage):
    def __init__(self) -> None:
        self.pod_bytes: Dict[PodId, bytes] = {}
        self.dep_pids: Dict[PodId, Set[PodId]] = {}

    def writer(self) -> PodWriter:
        return DictPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        return DictPodStorageReader(self)

    def estimate_size(self) -> int:
        # In memory size
        # import sys
        # return sys.getsizeof(self.pod_bytes) + sys.getsizeof(self.dep_pids)

        # Only bytes
        return sum(len(pod_bytes) for _, pod_bytes in self.pod_bytes.items())


""" File-based storage: each pod in one file """


@dataclass_json
@dataclass
class FilePodStorageStats:
    page_count: int


FilePodStorageIndex = Dict[PodId, int]
FilePodStoragePodPage = Dict[PodId, bytes]
FilePodStorageDepPage = Dict[PodId, Set[PodId]]


class FilePodStorageWriter(PodWriter):
    FLUSH_SIZE = 1_000_000  # 1 MB

    def __init__(self, storage: FilePodStorage) -> None:
        self.storage = storage
        self.pod_page_buffer: FilePodStoragePodPage = {}
        self.dep_page_buffer: FilePodStorageDepPage = {}
        self.pod_page_buffer_size: int = 0

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        self.pod_page_buffer[pod_id] = pod_bytes
        self.pod_page_buffer_size += len(pod_bytes)
        if self.pod_page_buffer_size > FilePodStorageWriter.FLUSH_SIZE:
            self.flush_pod()

    def write_dep(
        self,
        pod_id: PodId,
        dep_pids: Set[PodId],  # List of pids this pod depends on.
    ) -> None:
        self.dep_page_buffer[pod_id] = dep_pids

    def flush_pod(self) -> None:
        # WRite buffer.
        page_idx = self.storage.next_page_idx()
        with open(self.storage.page_path(page_idx), "wb") as f:
            pickle.dump(self.pod_page_buffer, f)

        # Update index.
        self.storage.update_index({pid: page_idx for pid in self.pod_page_buffer})

        # Reset buffer state.
        self.pod_page_buffer = {}
        self.pod_page_buffer_size = 0

    def flush_dep(self) -> None:
        # WRite buffer.
        page_idx = self.storage.next_page_idx()
        with open(self.storage.page_path(page_idx), "wb") as f:
            pickle.dump(self.dep_page_buffer, f)

        # Reset buffer state.
        self.dep_page_buffer = {}

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if len(self.pod_page_buffer) > 0:
            self.flush_pod()
        if len(self.dep_page_buffer) > 0:
            self.flush_dep()


class FilePodStorageReader(PodReader):
    def __init__(self, storage: FilePodStorage) -> None:
        self.storage = storage
        self.page_cache: Dict[int, FilePodStoragePodPage] = {}

    def read(self, pod_id: PodId) -> io.IOBase:
        page_idx = self.storage.search_index(pod_id)
        page_path = self.storage.page_path(page_idx)
        if page_idx not in self.page_cache:
            with open(page_path, "rb") as f:
                self.page_cache[page_idx] = pickle.load(f)
        page = self.page_cache[page_idx]
        assert pod_id in page, f"False index pointing {pod_id} to {page_path}: {page}"
        return io.BytesIO(page[pod_id])


class FilePodStorage(PodStorage):
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)

        self.stats = FilePodStorageStats(page_count=0)
        self.pid_index: FilePodStorageIndex = {}
        if self.is_init():
            self.stats = self.reload_stats()
            self.pid_index = self.reload_index()

    def writer(self) -> PodWriter:
        return FilePodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        # TODO: Leverage dep to prefetch relevant pods.
        return FilePodStorageReader(self)

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self.root_dir.glob("**/*") if f.is_file())

    def dep_path(self, pid: PodId) -> Path:
        return self.root_dir / f"{pid.tid}_{pid.oid}_dep.pkl"

    def next_page_idx(self) -> int:
        page_idx = self.stats.page_count
        self.stats.page_count += 1
        self.write_stats()
        return page_idx

    def page_path(self, page_idx: int) -> Path:
        return self.root_dir / f"page_{page_idx}.pkl"

    def stats_path(self) -> Path:
        return self.root_dir / "stats.json"

    def index_path(self) -> Path:
        return self.root_dir / "index.pkl"

    def write_stats(self) -> None:
        with open(self.stats_path(), "w") as f:
            f.write(self.stats.to_json())  # type: ignore

    def reload_stats(self) -> FilePodStorageStats:
        with open(self.stats_path(), "r") as f:
            return FilePodStorageStats.from_json(f.read())  # type: ignore

    def update_index(self, new_maps: Dict[PodId, int]) -> None:
        self.pid_index.update(new_maps)
        with open(self.index_path(), "wb") as f:
            pickle.dump(self.pid_index, f)

    def search_index(self, pid: PodId) -> int:
        return self.pid_index[pid]

    def reload_index(self) -> FilePodStorageIndex:
        with open(self.index_path(), "rb") as f:
            return pickle.load(f)

    def is_init(self) -> bool:
        return self.stats_path().exists() and self.index_path().exists()
