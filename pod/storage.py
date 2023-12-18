"""
Key-value storages with correlated/poset reads
"""

from __future__ import annotations

import io
from typing import Dict, List, Set

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
