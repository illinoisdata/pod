"""
Key-value storages with correlated/poset reads
"""

from __future__ import annotations  # isort:skip
import pod.__pickle__  ## noqa, isort:skip

import glob
import io
import os
import pickle
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import neo4j
import psycopg2
import pymongo
import redis
import xxhash
from dataclasses_json import dataclass_json
from loguru import logger
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from pod.algo import union_find
from pod.common import PodDependency, PodId

POD_CACHE_SIZE = 1_000_000_000


def serialize_pod_id(pod_id: PodId) -> bytes:
    return pickle.dumps(pod_id)


def deserialize_pod_id(serialized_pod_id: bytes) -> PodId:
    pid = pickle.loads(serialized_pod_id)
    return pid


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
        dep: PodDependency,
    ):
        raise NotImplementedError("Abstract method")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass  # Optional: Commit and tear down resources.


class PodReader:
    def __enter__(self) -> PodReader:
        return self  # Optional: Allocate resources.

    def read(self, pod_id: PodId) -> io.IOBase:
        raise NotImplementedError("Abstract method")

    def read_meta(self, pod_id: PodId) -> bytes:
        raise NotImplementedError("Abstract method")

    def dep_pids_by_rank(self) -> List[PodId]:
        raise NotImplementedError("Abstract method")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass  # Optional: Commit and tear down resources.


class PodStorage:
    def writer(self) -> PodWriter:
        raise NotImplementedError("Abstract method")

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        raise NotImplementedError("Abstract method")

    def connected_pods(self) -> Dict[PodId, PodId]:
        raise NotImplementedError("Abstract method")

    def estimate_size(self) -> int:
        raise NotImplementedError("Abstract method")


@dataclass
class PodBytesMemo:
    max_size: int
    min_size: int
    size: int
    memo_page: Dict[bytes, PodId]

    @staticmethod
    def new(max_size: int) -> PodBytesMemo:
        return PodBytesMemo(
            max_size=max_size,
            min_size=0,
            size=0,
            memo_page={},
        )

    def __contains__(self, pod_bytes: bytes) -> bool:
        return pod_bytes in self.memo_page

    def get(self, pod_bytes: bytes) -> PodId:
        return self.memo_page[pod_bytes]

    def put(self, pod_bytes: bytes, pod_id: PodId):
        if len(pod_bytes) < self.min_size or len(pod_bytes) > self.max_size or pod_bytes in self:
            return
        self.memo_page[pod_bytes] = pod_id
        self.size += len(pod_bytes)
        while self.size > self.max_size:
            popped_pod_bytes, _ = self.memo_page.popitem()
            self.size -= len(popped_pod_bytes)

    def __reduce__(self):
        return self.__class__, (self.max_size, self.size, self.memo_page)


PodIdSynonym = Dict[PodId, PodId]
SerializedPodId = bytes

""" Dictionary-based storage (ephemeral, for experimental uses) """


class DictPodStorageWriter(PodWriter):
    def __init__(self, storage: DictPodStorage) -> None:
        self.storage = storage
        self.new_deps: Dict[PodId, PodDependency] = {}

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        self.storage.pod_bytes[pod_id] = pod_bytes

    def write_dep(
        self,
        pod_id: PodId,
        dep: PodDependency,
    ) -> None:
        self.new_deps[pod_id] = dep

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.storage.update_deps(self.new_deps)


class DictPodStorageReader(PodReader):
    def __init__(self, storage: DictPodStorage, hint_pod_ids: List[PodId]) -> None:
        self.storage = storage
        self.hint_pod_ids = hint_pod_ids

    def read(self, pod_id: PodId) -> io.IOBase:
        return io.BytesIO(self.storage.pod_bytes[pod_id])

    def read_meta(self, pod_id: PodId) -> bytes:
        return self.storage.deps[pod_id].meta

    def dep_pids_by_rank(self) -> List[PodId]:
        seen_pid: Set[PodId] = set()
        pid_queue: Queue[PodId] = Queue()
        for pid in self.hint_pod_ids:
            seen_pid.add(pid)
            pid_queue.put(pid)
        while not pid_queue.empty():
            pid = pid_queue.get()
            for dep_pid in self.storage.deps[pid].dep_pids:
                if dep_pid not in seen_pid:
                    seen_pid.add(dep_pid)
                    pid_queue.put(dep_pid)
        return sorted(seen_pid, key=lambda pid: self.storage.deps[pid].rank)


class DictPodStorage(PodStorage):
    def __init__(self) -> None:
        self.pod_bytes: Dict[PodId, bytes] = {}
        self.deps: Dict[PodId, PodDependency] = {}
        self.roots: Dict[PodId, PodId] = {}

    def writer(self) -> PodWriter:
        return DictPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        return DictPodStorageReader(self, hint_pod_ids)

    def connected_pods(self) -> Dict[PodId, PodId]:
        return self.roots  # Assuming filtering/selection above.
        # return {pid: self.roots.get(pid, pid) for pid in pod_ids}

    def update_deps(self, new_deps: FilePodStoragePodIdDep) -> None:
        self.deps.update(new_deps)
        for pid, root_pid in union_find(new_deps):
            self.roots[pid] = root_pid

    def estimate_size(self) -> int:
        # In memory size
        # import sys
        # return sys.getsizeof(self.pod_bytes) + sys.getsizeof(self.deps)

        # Only bytes
        return sum(len(pod_bytes) for _, pod_bytes in self.pod_bytes.items())


""" File-based storage: each pod in one file """


FilePodStorageIndex = Dict[PodId, int]
FilePodStoragePodIdDep = Dict[PodId, PodDependency]
FilePodStoragePodPage = Dict[PodId, bytes]
FilePodStorageDepPage = FilePodStoragePodIdDep


@dataclass_json
@dataclass
class FilePodStorageStats:
    pod_page_count: int
    dep_page_count: int
    pid_index_page_count: int
    pid_synonym_page_count: int


# max_tid_diff: int = 0  # stat_max_tid_diff


class FilePodStorageWriter(PodWriter):
    FLUSH_SIZE = 1_000_000  # 1 MB
    SMALL_POD_SIZE = 1024  # 1 KB

    def __init__(self, storage: FilePodStorage) -> None:
        self.storage = storage
        self.new_pid_index: FilePodStorageIndex = {}
        self.new_pid_synonyms: Dict[PodId, PodId] = {}
        self.pod_page_buffer: FilePodStoragePodPage = {}
        self.dep_page_buffer: FilePodStorageDepPage = {}
        self.pod_page_buffer_size: int = 0

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        pod_bytes_key = xxhash.xxh3_128_digest(pod_bytes) if self.storage.memo_hash else pod_bytes
        if pod_bytes_key in self.storage.pod_bytes_memo:
            # Save as synonymous pids.
            same_pod_id = self.storage.pod_bytes_memo.get(pod_bytes_key)
            self.new_pid_synonyms[pod_id] = same_pod_id
            # global max_tid_diff  # stat_max_tid_diff
            # if max_tid_diff < pod_id.tid - same_pod_id.tid:  # stat_max_tid_diff
            #     max_tid_diff = pod_id.tid - same_pod_id.tid  # stat_max_tid_diff
            #     print(max_tid_diff, pod_id, same_pod_id, len(pod_bytes))  # stat_max_tid_diff
        else:
            # New pod bytes.
            self.storage.pod_bytes_memo.put(pod_bytes_key, pod_id)

            # Write to buffer.
            if len(pod_bytes) > FilePodStorageWriter.SMALL_POD_SIZE:
                # Write bigger pod bytes individually.
                self.flush_pod_impl({pod_id: pod_bytes})
            else:
                self.pod_page_buffer[pod_id] = pod_bytes
                self.pod_page_buffer_size += len(pod_bytes)
                if self.pod_page_buffer_size > FilePodStorageWriter.FLUSH_SIZE:
                    self.flush_pod()

    def write_dep(
        self,
        pod_id: PodId,
        dep: PodDependency,
    ) -> None:
        self.dep_page_buffer[pod_id] = dep

    def flush_pod(self) -> None:
        self.flush_pod_impl(self.pod_page_buffer)
        self.pod_page_buffer = {}
        self.pod_page_buffer_size = 0

    def flush_pod_impl(self, pod_page_buffer: FilePodStoragePodPage):
        # Write buffer.
        page_idx = self.storage.next_pod_page_idx()
        with open(self.storage.pod_page_path(page_idx), "wb") as f:
            pickle.dump(pod_page_buffer, f)

        # Update index.
        self.new_pid_index.update({pid: page_idx for pid in pod_page_buffer})

    def flush_dep(self) -> None:
        # Write buffer.
        page_idx = self.storage.next_dep_page_idx()
        with open(self.storage.dep_page_path(page_idx), "wb") as f:
            pickle.dump(self.dep_page_buffer, f)

        # Save to in-memory storage.
        self.storage.update_deps(self.dep_page_buffer)

        # Reset buffer state.
        self.dep_page_buffer = {}

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if len(self.pod_page_buffer) > 0:
            self.flush_pod()
        if len(self.dep_page_buffer) > 0:
            self.flush_dep()
        if len(self.new_pid_index) > 0:
            self.storage.update_index(self.new_pid_index)
        if len(self.new_pid_synonyms) > 0:
            self.storage.update_pid_synonym(self.new_pid_synonyms)
        # logger.warning(f"memo_size= {self.storage.pod_bytes_memo.size}")


class FilePodStorageReader(PodReader):
    def __init__(self, storage: FilePodStorage, expected_pids: Set[PodId], page_idxs: Set[int]) -> None:
        # from pod.stats import PodCacheStat  # stat_cache_pfl
        # self.cache_stat = PodCacheStat()  # stat_cache_pfl

        self.storage = storage
        self.expected_pids = expected_pids
        self.page_cache: Dict[int, FilePodStoragePodPage] = {}
        for page_idx in page_idxs:
            with open(self.storage.pod_page_path(page_idx), "rb") as f:
                self.page_cache[page_idx] = pickle.load(f)
                # self.cache_stat.add_io(  # stat_cache_pfl
                # sum(len(pod_bytes)  # stat_cache_pfl
                # for _, pod_bytes in self.page_cache[page_idx].items()))  # stat_cache_pfl

    def read(self, pod_id: PodId) -> io.IOBase:
        resolved_pid = self.storage.resolve_pid_synonym(pod_id)
        page_idx = self.storage.search_index(resolved_pid)
        page_path = self.storage.pod_page_path(page_idx)
        if page_idx not in self.page_cache:
            logger.warning(f"Unexpected cache miss on {pod_id} (={resolved_pid}), page_idx= {page_path}")
            with open(page_path, "rb") as f:
                self.page_cache[page_idx] = pickle.load(f)
                # self.cache_stat.add_io(  # stat_cache_pfl
                # sum(len(pod_bytes)  # stat_cache_pfl
                # for _, pod_bytes in self.page_cache[page_idx].items()))  # stat_cache_pfl
        page = self.page_cache[page_idx]
        if resolved_pid not in page:
            raise ValueError(f"False index pointing {pod_id} ({resolved_pid}) to {page_path}: {page.keys()}")
        # self.cache_stat.add_read(str(resolved_pid), len(page[resolved_pid]))  # stat_cache_pfl
        return io.BytesIO(page[resolved_pid])

    def read_meta(self, pod_id: PodId) -> bytes:
        return self.storage.deps[pod_id].meta

    def dep_pids_by_rank(self) -> List[PodId]:
        return sorted(self.expected_pids, key=self.storage.pid_rank)

    # def __del__(self) -> None:  # stat_cache_pfl
    #     self.cache_stat.summary()  # stat_cache_pfl


class FilePodStorage(PodStorage):
    def __init__(self, root_dir: Path, memo_hash: bool = True) -> None:
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.memo_hash = memo_hash

        self.stats = FilePodStorageStats(
            pod_page_count=0,
            dep_page_count=0,
            pid_index_page_count=0,
            pid_synonym_page_count=0,
        )
        self.pid_index: FilePodStorageIndex = {}
        self.pid_synonym: PodIdSynonym = {}
        self.pod_bytes_memo: PodBytesMemo = PodBytesMemo.new(POD_CACHE_SIZE)
        self.deps: FilePodStoragePodIdDep = {}
        self.roots: Dict[PodId, PodId] = {}
        if self.is_init():
            self.stats = self.reload_stats()
            self.pid_index = self.reload_index()
            self.pid_synonym = self.reload_pid_synonym()
            self.reload_pod_bytes_memo()
            self.deps = self.reload_pid_deps()

    def writer(self) -> PodWriter:
        return FilePodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        seen_pid: Set[PodId] = set()
        pid_queue: Queue[PodId] = Queue()
        for pid in hint_pod_ids:
            seen_pid.add(pid)
            pid_queue.put(pid)
        page_idxs: Set[int] = set()
        while not pid_queue.empty():
            pid = pid_queue.get()
            resolved_pid = self.resolve_pid_synonym(pid)
            page_idx = self.search_index(resolved_pid)
            page_idxs.add(page_idx)
            for dep_pid in self.deps[pid].dep_pids:
                if dep_pid not in seen_pid:
                    seen_pid.add(dep_pid)
                    pid_queue.put(dep_pid)
        return FilePodStorageReader(self, seen_pid, page_idxs)

    def connected_pods(self) -> Dict[PodId, PodId]:
        return self.roots  # Assuming filtering/selection above.
        # return {pid: self.roots.get(pid, pid) for pid in pod_ids}

    def update_deps(self, new_deps: FilePodStoragePodIdDep) -> None:
        self.deps.update(new_deps)
        for (tid, oid), (root_tid, root_oid) in union_find(new_deps):
            pid = PodId(tid, oid)
            root_pid = PodId(root_tid, root_oid)
            self.roots[pid] = root_pid

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self.root_dir.glob("**/*") if f.is_file())

    def next_pod_page_idx(self) -> int:
        page_idx = self.stats.pod_page_count
        self.stats.pod_page_count += 1
        self.write_stats()
        return page_idx

    def next_dep_page_idx(self) -> int:
        page_idx = self.stats.dep_page_count
        self.stats.dep_page_count += 1
        self.write_stats()
        return page_idx

    def next_pid_index_page_idx(self) -> int:
        page_idx = self.stats.pid_index_page_count
        self.stats.pid_index_page_count += 1
        self.write_stats()
        return page_idx

    def next_pid_synonym_page_idx(self) -> int:
        page_idx = self.stats.pid_synonym_page_count
        self.stats.pid_synonym_page_count += 1
        self.write_stats()
        return page_idx

    def pod_page_path(self, page_idx: int) -> Path:
        return self.root_dir / f"pod_{page_idx}.pkl"

    def dep_page_path(self, page_idx: int) -> Path:
        return self.root_dir / f"dep_{page_idx}.pkl"

    def stats_path(self) -> Path:
        return self.root_dir / "stats.json"

    def index_path(self, page_idx: int) -> Path:
        return self.root_dir / f"index_{page_idx}.pkl"

    def pid_synonym_path(self, page_idx: int) -> Path:
        return self.root_dir / f"pid_synonym_{page_idx}.pkl"

    def write_stats(self) -> None:
        with open(self.stats_path(), "w") as f:
            f.write(self.stats.to_json())  # type: ignore

    def reload_stats(self) -> FilePodStorageStats:
        with open(self.stats_path(), "r") as f:
            return FilePodStorageStats.from_json(f.read())  # type: ignore

    def update_index(self, new_pid_index: FilePodStorageIndex) -> None:
        self.pid_index.update(new_pid_index)
        self.write_index(new_pid_index)

    def search_index(self, pid: PodId) -> int:
        return self.pid_index[pid]

    def write_index(self, new_pid_index: FilePodStorageIndex) -> None:
        page_idx = self.next_pid_index_page_idx()
        with open(self.index_path(page_idx), "wb") as f:
            pickle.dump(new_pid_index, f)

    def reload_index(self) -> FilePodStorageIndex:
        pid_index: FilePodStorageIndex = {}
        for page_idx in range(self.stats.pid_index_page_count):
            with open(self.index_path(page_idx), "rb") as f:
                pid_index.update(pickle.load(f))
        return pid_index

    def update_pid_synonym(self, new_pid_synonyms: PodIdSynonym) -> None:
        self.pid_synonym.update(new_pid_synonyms)
        self.write_pid_synonym(new_pid_synonyms)

    def resolve_pid_synonym(self, pid: PodId) -> PodId:
        return self.pid_synonym.get(pid, pid)

    def write_pid_synonym(self, pid_synonym: PodIdSynonym) -> None:
        page_idx = self.next_pid_synonym_page_idx()
        with open(self.pid_synonym_path(page_idx), "wb") as f:
            pickle.dump(pid_synonym, f)

    def pid_rank(self, pid: PodId) -> int:
        return self.deps[pid].rank

    def reload_pid_synonym(self) -> PodIdSynonym:
        pid_synonym: PodIdSynonym = {}
        for page_idx in range(self.stats.pid_synonym_page_count):
            with open(self.pid_synonym_path(page_idx), "rb") as f:
                pid_synonym.update(pickle.load(f))
        return pid_synonym

    def reload_pod_bytes_memo(self):
        for page_path in self.root_dir.glob("pod_*.pkl"):
            with open(page_path, "rb") as f:
                page = pickle.load(f)
            for pod_id, pod_bytes in page.items():
                self.pod_bytes_memo.put(pod_bytes, pod_id)

    def reload_pid_deps(self) -> FilePodStoragePodIdDep:
        pid_deps: FilePodStoragePodIdDep = {}
        for page_path in self.root_dir.glob("dep_*.pkl"):
            with open(page_path, "rb") as f:
                page = pickle.load(f)
            for pod_id, deps in page.items():
                pid_deps[pod_id] = deps
        return pid_deps

    def is_init(self) -> bool:
        return self.stats_path().exists() and self.index_path(0).exists()


""" PostgreSQL storage: each pod as an entry in a database """


class PostgreSQLPodStorageWriter(PodWriter):
    CHUNK_SIZE = 100_000_000  # 90 MB
    FLUSH_SIZE = 500_000_000  # 5 GB

    def __init__(self, storage: PostgreSQLPodStorage) -> None:
        self.storage = storage
        self.storage_buffer: List[Tuple] = []
        self.dependency_buffer: List[Tuple] = []
        self.buf_size = 0
        self.new_pid_synonyms: List[Tuple] = []
        self.rank_buffer: List[Tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.storage.db_conn.rollback()
        else:
            self._flush_synonyms()
            self._flush_storage()
            self._flush_dependencies()
            self._flush_ranks()
            self.storage.db_conn.commit()

    def _flush_storage(self):
        if self.buf_size == 0:
            return
        with self.storage.db_conn.cursor() as cursor:
            query = """
                INSERT INTO pod_storage (tid, oid, chunk, pod_bytes)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tid, oid, chunk) DO UPDATE SET pod_bytes = EXCLUDED.pod_bytes;
            """
            cursor.executemany(query, self.storage_buffer)
        self.storage_buffer = []
        self.buf_size = 0

    def _flush_dependencies(self):
        if len(self.dependency_buffer) == 0:
            return
        with self.storage.db_conn.cursor() as cursor:
            insert_values = ",".join(cursor.mogrify("(%s,%s,%s,%s)", x).decode() for x in self.dependency_buffer)
            if insert_values:
                insert_query = f"""
                    INSERT INTO pod_dependencies (pod_id_tid, pod_id_oid, dep_pid_tid, dep_pid_oid)
                    VALUES {insert_values}
                    ON CONFLICT DO NOTHING
                """
                cursor.execute(insert_query)
        self.dependency_buffer = []

    def _flush_synonyms(self):
        if len(self.new_pid_synonyms) == 0:
            return
        with self.storage.db_conn.cursor() as cursor:
            insert_values = ",".join(cursor.mogrify("(%s,%s,%s,%s)", x).decode() for x in self.new_pid_synonyms)
            if insert_values:
                insert_query = f"""
                    INSERT INTO pod_synonyms (pod_tid, pod_oid, syn_tid, syn_oid)
                    VALUES {insert_values};
                """
                cursor.execute(insert_query)
        self.storage.synonyms.update({PodId(r[0], r[1]): PodId(r[2], r[3]) for r in self.new_pid_synonyms})
        self.new_pid_synonyms = []

    def _flush_ranks(self):
        if len(self.rank_buffer) == 0:
            return
        with self.storage.db_conn.cursor() as cursor:
            insert_values = ",".join(cursor.mogrify("(%s,%s,%s)", x).decode() for x in self.rank_buffer)
            if insert_values:
                insert_query = f"""
                    INSERT INTO pod_ranks (tid, oid, rank)
                    VALUES {insert_values};
                """
                cursor.execute(insert_query)
        self.rank_buffer = []

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        if pod_bytes in self.storage.pod_bytes_memo:
            # Save as synonymous pids.
            same_pod_id = self.storage.pod_bytes_memo.get(pod_bytes)
            self.new_pid_synonyms.append((pod_id.tid, pod_id.oid, same_pod_id.tid, same_pod_id.oid))
        else:
            # New pod bytes.
            pod_bytes_memview = memoryview(pod_bytes)
            self.storage.pod_bytes_memo.put(pod_bytes, pod_id)
            for i in range(0, len(pod_bytes), PostgreSQLPodStorageWriter.CHUNK_SIZE):
                self.buf_size += min(PostgreSQLPodStorageWriter.CHUNK_SIZE, len(pod_bytes) - i)
                self.storage_buffer.append(
                    (
                        pod_id.tid,
                        pod_id.oid,
                        int(i / PostgreSQLPodStorageWriter.CHUNK_SIZE),
                        pod_bytes_memview[i : i + PostgreSQLPodStorageWriter.CHUNK_SIZE],
                    )
                )
                if self.buf_size > PostgreSQLPodStorageWriter.FLUSH_SIZE:
                    self._flush_storage()

    def write_dep(
        self,
        pod_id: PodId,
        dep: PodDependency,
    ) -> None:
        self.dependency_buffer += [(pod_id.tid, pod_id.oid, p.tid, p.oid) for p in dep.dep_pids]
        self.rank_buffer += [(pod_id.tid, pod_id.oid, dep.rank)]


class PostgreSQLPodStorageReader(PodReader):
    def __init__(
        self, storage: PostgreSQLPodStorage, hint_pod_ids: List[PodId], cache: Dict[Tuple, bytearray], ranks: List[PodId]
    ) -> None:
        self.storage = storage
        self.hint_pod_ids = hint_pod_ids
        self.cache = cache
        self.ranks = ranks

    def read(self, pod_id: PodId) -> io.IOBase:
        pod_id = self.storage.synonyms.get(pod_id, pod_id)
        if (pod_id.tid, pod_id.oid) not in self.cache:
            with self.storage.db_conn.cursor() as cursor:
                cursor.execute(
                    "SELECT pod_bytes FROM pod_storage WHERE tid = %s AND oid = %s ORDER BY chunk",
                    (pod_id.tid, pod_id.oid),
                )
                result = cursor.fetchall()
                if len(result) == 0:
                    raise ValueError(f"No data found for the given pod_id {pod_id}")
                self.cache[(pod_id.tid, pod_id.oid)] = bytearray()
                for item in result:
                    self.cache[(pod_id.tid, pod_id.oid)].extend(item[0])
        return io.BytesIO(self.cache[(pod_id.tid, pod_id.oid)])

    def dep_pids_by_rank(self) -> List[PodId]:
        return self.ranks


class PostgreSQLPodStorage(PodStorage):
    def __init__(self, host: str, port: int) -> None:
        PostgreSQLPodStorage._create_pod_db_if_has_not(host, port)
        self.synonyms: PodIdSynonym = {}
        try:
            self.db_conn = psycopg2.connect(
                dbname="pod",
                user="postgres",
                password="postgres",
                host=host,
                port=port,
            )
        except psycopg2.OperationalError as e:
            logger.error(f"Error connecting to PostgreSQL, {e}")
            raise
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pod_storage (
                    tid BIGINT,
                    oid BIGINT,
                    pod_bytes BYTEA,
                    chunk INTEGER,
                    PRIMARY KEY (tid, oid, chunk)
                );
                CREATE TABLE IF NOT EXISTS pod_ranks (
                    tid BIGINT,
                    oid BIGINT,
                    rank INTEGER,
                    PRIMARY KEY (tid, oid)
                );
                CREATE TABLE IF NOT EXISTS pod_synonyms (
                    pod_tid BIGINT,
                    pod_oid BIGINT,
                    syn_tid BIGINT,
                    syn_oid BIGINT,
                    PRIMARY KEY (pod_tid, pod_oid, syn_tid, syn_oid)
                );
                CREATE TABLE IF NOT EXISTS pod_dependencies (
                    pod_id_tid BIGINT,
                    pod_id_oid BIGINT,
                    dep_pid_tid BIGINT,
                    dep_pid_oid BIGINT,
                    PRIMARY KEY (pod_id_tid, pod_id_oid, dep_pid_tid, dep_pid_oid)
                );


                CREATE OR REPLACE FUNCTION get_dependencies(hint_pod_ids BIGINT[][])
                    RETURNS TABLE(tid BIGINT, oid BIGINT, pod_bytes BYTEA, chunk INTEGER, rank INTEGER)
                    LANGUAGE plpgsql
                    AS $$
                    DECLARE
                        node_tid BIGINT;
                        node_oid BIGINT;
                        iteration BOOLEAN := FALSE;
                        dep_record RECORD;
                        curr_record RECORD;
                    BEGIN
                        -- Temporary table to store visited nodes and their levels
                        CREATE TEMP TABLE IF NOT EXISTS current_level_it_0 (
                            tid BIGINT,
                            oid BIGINT,
                            PRIMARY KEY (tid, oid)
                        );
                        TRUNCATE TABLE current_level_it_0;

                        CREATE TEMP TABLE IF NOT EXISTS all_nodes (
                            tid BIGINT,
                            oid BIGINT,
                            PRIMARY KEY (tid, oid)
                        );
                        TRUNCATE TABLE all_nodes;

                        CREATE TEMP TABLE IF NOT EXISTS current_level_it_1 (
                            tid BIGINT,
                            oid BIGINT,
                            PRIMARY KEY (tid, oid)
                        );
                        TRUNCATE TABLE current_level_it_1;

                        -- Initialize current level
                        FOR i IN 1..array_upper(hint_pod_ids, 1) LOOP
                            node_tid := hint_pod_ids[i][1];
                            node_oid := hint_pod_ids[i][2];
                            INSERT INTO current_level_it_0 (tid, oid) VALUES (node_tid, node_oid);
                            INSERT INTO all_nodes (tid, oid) VALUES (node_tid, node_oid);
                        END LOOP;

                        -- Recursive traversal
                        LOOP
                            IF iteration THEN
                                -- Exit when no more nodes to process at the current level
                                EXIT WHEN NOT (SELECT EXISTS (SELECT 1 FROM current_level_it_1));
                                -- Process nodes at the current level
                                FOR curr_record IN SELECT cl.tid, cl.oid FROM current_level_it_1 cl LOOP
                                    -- Find dependencies of the current node
                                    FOR dep_record IN SELECT pd.dep_pid_tid, pd.dep_pid_oid
                                                            FROM pod_dependencies pd
                                                            WHERE pd.pod_id_tid = curr_record.tid
                                                                AND pd.pod_id_oid = curr_record.oid LOOP
                                        -- Insert the dependency with the next level, if not already in visited_nodes
                                        IF NOT EXISTS (SELECT 1 FROM all_nodes an WHERE an.tid = dep_record.dep_pid_tid
                                                AND an.oid = dep_record.dep_pid_oid) THEN
                                            INSERT INTO current_level_it_0 (tid, oid)
                                            VALUES (dep_record.dep_pid_tid, dep_record.dep_pid_oid);
                                            INSERT INTO all_nodes (tid, oid)
                                            VALUES (dep_record.dep_pid_tid, dep_record.dep_pid_oid);
                                        END IF;

                                    END LOOP;
                                END LOOP;
                                TRUNCATE TABLE current_level_it_1;
                            ELSE
                                -- Exit when no more nodes to process at the current level
                                EXIT WHEN NOT (SELECT EXISTS (SELECT 1 FROM current_level_it_0));

                                -- Process nodes at the current level
                                FOR curr_record IN SELECT cl.tid, cl.oid FROM current_level_it_0 cl LOOP
                                    -- Find dependencies of the current node
                                    FOR dep_record IN SELECT pd.dep_pid_tid, pd.dep_pid_oid
                                                            FROM pod_dependencies pd
                                                            WHERE pd.pod_id_tid = curr_record.tid AND
                                                                    pd.pod_id_oid = curr_record.oid LOOP
                                        -- Insert the dependency with the next level, if not already in visited_nodes
                                        IF NOT EXISTS (SELECT 1 FROM all_nodes an WHERE an.tid = dep_record.dep_pid_tid
                                                                                    AND an.oid = dep_record.dep_pid_oid) THEN
                                            INSERT INTO current_level_it_1 (tid, oid)
                                            VALUES (dep_record.dep_pid_tid, dep_record.dep_pid_oid);

                                            -- Insert the same dependency into other_table as well
                                            INSERT INTO all_nodes (tid, oid)
                                            VALUES (dep_record.dep_pid_tid, dep_record.dep_pid_oid);
                                        END IF;

                                    END LOOP;
                                END LOOP;
                                TRUNCATE TABLE current_level_it_0;
                            END IF;

                            -- Move to the next level
                            iteration := NOT iteration;
                        END LOOP;

                        -- Return the result
                        RETURN QUERY SELECT
                            combined_results.tid,
                            combined_results.oid,
                            combined_results.pod_bytes,
                            combined_results.chunk,
                            combined_results.rank
                        FROM
                            (
                                SELECT
                                    pr.tid AS tid, pr.oid AS oid, ps.pod_bytes AS pod_bytes, ps.chunk AS chunk, pr.rank AS rank
                                FROM
                                    (pod_ranks pr INNER JOIN all_nodes an ON (pr.tid, pr.oid) = (an.tid, an.oid))
                                    LEFT JOIN pod_storage ps ON (pr.tid, pr.oid) = (ps.tid, ps.oid)
                                UNION ALL
                                SELECT
                                    ps.tid AS tid, ps.oid AS oid, ps.pod_bytes AS pod_bytes, ps.chunk AS chunk, -1 AS rank
                                FROM
                                    (all_nodes an INNER JOIN pod_synonyms syn ON (syn.pod_tid, syn.pod_oid) = (an.tid, an.oid))
                                    INNER JOIN pod_storage ps ON (ps.tid, ps.oid) = (syn.syn_tid, syn.syn_oid)
                            ) AS combined_results
                        ORDER BY
                            combined_results.tid,
                            combined_results.oid,
                            combined_results.chunk;
                    END;
                    $$;
            """
            )
            self._get_synonyms_from_db(cursor)
            self.db_conn.commit()
        self.pod_bytes_memo: PodBytesMemo = PodBytesMemo.new(POD_CACHE_SIZE)

    @staticmethod
    def _create_pod_db_if_has_not(host: str, port: int) -> None:
        try:
            db_conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="postgres",
                host=host,
                port=port,
            )
            db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = db_conn.cursor()
            cursor.execute("CREATE DATABASE pod")
        except psycopg2.errors.DuplicateDatabase:
            return  # Previous run has already created this database.
        except psycopg2.OperationalError as e:
            logger.error(f"Error creating pod database, {e}")
            raise e

    def _get_synonyms_from_db(self, cursor: psycopg2.extensions.cursor):
        cursor.execute("SELECT * FROM pod_synonyms;")
        results = cursor.fetchall()
        for row in results:
            tid, oid, syn_tid, syn_oid = row
            self.synonyms[PodId(tid, oid)] = PodId(syn_tid, syn_oid)

    def _prefetch_bytes_and_ranks(self, cursor: psycopg2.extensions.cursor, hint_tid_oid_array: List[List]):
        cursor.execute("SELECT * FROM get_dependencies(%s::BIGINT[][]);", (hint_tid_oid_array,))
        results = cursor.fetchall()
        cache = {}
        ranks = {}
        for row in results:
            tid, oid, pod_bytes, chunk, rank = row
            if pod_bytes:
                if (tid, oid) not in cache:
                    cache[(tid, oid)] = bytearray()
                cache[(tid, oid)].extend(pod_bytes)
            if rank != -1:  # Rank is -1 for pods that are only fetched as synonyms
                ranks[PodId(tid, oid)] = int(rank)
        return cache, sorted([k for k in ranks.keys()], key=lambda p: ranks[p])

    def writer(self) -> PodWriter:
        return PostgreSQLPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        cache = {}
        if len(hint_pod_ids) > 0:
            hint_tid_oid_array = [[p.tid, p.oid] for p in hint_pod_ids]
            with self.db_conn.cursor() as cursor:
                cache, ranks = self._prefetch_bytes_and_ranks(cursor, hint_tid_oid_array)
        return PostgreSQLPodStorageReader(self, hint_pod_ids, cache, ranks)

    def estimate_size(self) -> int:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_total_relation_size('pod_dependencies') + pg_total_relation_size('pod_storage')
                           AS total_size;
            """
            )
            size = cursor.fetchone()[0]
        return size

    def __del__(self):
        if hasattr(self, "db_conn"):
            self.db_conn.close()


""" Redis storage """


class RedisPodStorageWriter(PodWriter):
    def __init__(self, storage: RedisPodStorage) -> None:
        self.storage = storage
        self.pod_data: Dict[PodId, bytes] = {}
        self.dependency_map: Dict[PodId, Set[str]] = {}
        self.new_deps: FilePodStoragePodIdDep = {}
        self.new_pid_synonyms: Dict[PodId, PodId] = {}
        self.new_ranks: Dict[PodId, int] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.storage.redis_client.pipeline() as pipe:
            for pod_id, pod_bytes in self.pod_data.items():  # Every pod written is either in pod data or pod synonyms
                pipe.set(f"pod_bytes:{pod_id.redis_str()}", pod_bytes)
                if pod_id in self.dependency_map:
                    if len(self.dependency_map[pod_id]) > 0:
                        pipe.sadd(f"dep_pids:{pod_id.redis_str()}", *(self.dependency_map[pod_id]))
                    pipe.set(f"pod_ranks:{pod_id.redis_str()}", self.new_ranks[pod_id])
            for pod_id, syn_pid in self.new_pid_synonyms.items():
                self.storage.synonyms[pod_id] = syn_pid
                pipe.set(f"pod_synonyms:{pod_id.redis_str()}", syn_pid.redis_str())
                if pod_id in self.dependency_map:
                    if len(self.dependency_map[pod_id]) > 0:
                        pipe.sadd(f"dep_pids:{pod_id.redis_str()}", *(self.dependency_map[pod_id]))
                    pipe.set(f"pod_ranks:{pod_id.redis_str()}", self.new_ranks[pod_id])
            pipe.execute()
        self.storage.update_deps(self.new_deps)
        self.new_pid_synonyms = {}
        self.dependency_map = {}
        self.pod_data = {}
        self.new_ranks = {}

    def write_pod(self, pod_id: PodId, pod_bytes: bytes) -> None:
        if pod_bytes in self.storage.pod_bytes_memo:
            self.new_pid_synonyms[pod_id] = self.storage.pod_bytes_memo.get(pod_bytes)
        else:
            self.storage.pod_bytes_memo.put(pod_bytes, pod_id)
            self.pod_data[pod_id] = pod_bytes

    def write_dep(self, pod_id: PodId, dep: PodDependency) -> None:
        self.new_deps[pod_id] = dep
        serialized_dep_pids = {pid.redis_str() for pid in dep.dep_pids}
        self.dependency_map[pod_id] = serialized_dep_pids
        self.new_ranks[pod_id] = dep.rank


class RedisPodStorageReader(PodReader):
    def __init__(
        self, storage: RedisPodStorage, hint_pod_ids: List[PodId], cache: Dict[PodId, bytes], ranks: Dict[PodId, int]
    ) -> None:
        self.storage = storage
        self.hint_pod_ids = hint_pod_ids
        self.cache = cache
        self.ranks = ranks

    def read(self, pod_id: PodId) -> io.IOBase:
        pod_id = self.storage.synonyms.get(pod_id, pod_id)
        if pod_id not in self.cache:
            pod_bytes = self.storage.redis_client.get(f"pod_bytes:{pod_id.redis_str()}")
            pod_bytes = cast(bytes, pod_bytes)
            if pod_bytes is None:
                raise KeyError(f"Data not found for Pod ID: {pod_id}")
            self.cache[pod_id] = pod_bytes
        return io.BytesIO(self.cache[pod_id])

    def read_meta(self, pod_id: PodId) -> bytes:
        return self.storage.deps[pod_id].meta

    def dep_pids_by_rank(self) -> List[PodId]:
        return sorted(self.ranks.keys(), key=lambda k: self.ranks[k])


class RedisPodStorage(PodStorage):
    def __init__(self, host: str, port: int) -> None:
        self.redis_client = redis.Redis(host=host, port=port)
        self.pod_bytes_memo: PodBytesMemo = PodBytesMemo.new(POD_CACHE_SIZE)
        self.synonyms: PodIdSynonym = {}
        self.deps: FilePodStoragePodIdDep = {}
        self.roots: Dict[PodId, PodId] = {}
        self._fetch_synonyms()

    def _fetch_synonyms(self):
        pipeline = self.redis_client.pipeline()
        for key in self.redis_client.scan_iter("pod_synonyms:*"):
            pipeline.get(key)
        results = pipeline.execute()
        for key, synonym in zip(self.redis_client.scan_iter("pod_synonyms:*"), results):
            key = key.decode("utf-8")
            pid_redis_str = key.split(":")[1]
            pod_id = PodId.from_redis_str(pid_redis_str)
            self.synonyms[pod_id] = PodId.from_redis_str(synonym)

    def writer(self) -> PodWriter:
        return RedisPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        loaded_pod_bytes: Dict[PodId, bytes] = {}
        pid_ranks: Dict[PodId, int] = {}
        current_level = list(hint_pod_ids)
        pipe = self.redis_client.pipeline()
        all_pod_ids = set(current_level)
        while current_level:
            next_level = []
            for p in current_level:
                pipe.smembers(f"dep_pids:{p.redis_str()}")
                pipe.get(f"pod_ranks:{p.redis_str()}")
                pipe.get(f"pod_bytes:{p.redis_str()}")
            results = pipe.execute()
            if results:
                for i in range(0, len(results), 3):
                    deps, rank, pod_bytes = results[i], results[i + 1], results[i + 2]
                    deps, rank = results[i], results[i + 1]
                    pod_bytes = cast(bytes, pod_bytes)
                    if pod_bytes:
                        loaded_pod_bytes[current_level[i // 3]] = pod_bytes
                    if rank is not None:
                        pid_ranks[current_level[i // 3]] = int(rank)
                    for pod_id_str in deps:
                        made_pod_id = PodId.from_redis_str(pod_id_str.decode("utf-8"))
                        if made_pod_id not in all_pod_ids:
                            all_pod_ids.add(made_pod_id)
                            next_level.append(made_pod_id)
            current_level = next_level

        for k in all_pod_ids:
            pipe.get(f"pod_synonyms:{k.redis_str()}")
        syns = pipe.execute()
        syn_pod_ids = []
        for s in syns:
            if s:
                s = s.decode("utf-8")
                pipe.get(f"pod_bytes:{s}")
                syn_pod_ids.append(s)
        syn_results = pipe.execute()
        for i in range(len(syn_results)):
            pod_bytes = syn_results[i]
            loaded_pod_bytes[PodId.from_redis_str(syn_pod_ids[i])] = pod_bytes
        return RedisPodStorageReader(self, hint_pod_ids, loaded_pod_bytes, pid_ranks)

    def connected_pods(self) -> Dict[PodId, PodId]:
        return self.roots  # Assuming filtering/selection above.
        # return {pid: self.roots.get(pid, pid) for pid in pod_ids}

    def update_deps(self, new_deps: FilePodStoragePodIdDep) -> None:
        self.deps.update(new_deps)
        for pid, root_pid in union_find(new_deps):
            self.roots[pid] = root_pid

    def estimate_size(self) -> int:
        memory_data = self.redis_client.info("memory")
        if memory_data is None:
            raise RuntimeError("Error estimating size")
        memory_data = cast(Dict[str, Any], memory_data)
        return memory_data["used_memory"]


"""Neo4j pod storage"""


class Neo4jPodStorageWriter(PodWriter):
    def __init__(self, storage: Neo4jPodStorage) -> None:
        self.storage = storage
        self.pod_data: List[Tuple[bytes, bytes]] = []
        self.dependencies: List[Tuple[bytes, bytes]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush_data()

    def flush_data(self):
        with self.storage.session() as session:
            with session.begin_transaction() as tx:
                # Write the pod
                tx.run(
                    "UNWIND $pods_list AS pod " "CREATE (p:Pod {pod_id: pod[0], pod_bytes: pod[1]}) ", pods_list=self.pod_data
                )

                # Write all dependencies at once
                tx.run(
                    "UNWIND $deps_list AS dep "
                    "MATCH (p:Pod {pod_id: dep[0]}) "
                    "MATCH (depPod:Pod {pod_id: dep[1]}) "
                    "CREATE (p)-[:DEPENDS_ON]->(depPod)",
                    deps_list=self.dependencies,
                )
                tx.commit()

    def write_pod(self, pod_id: PodId, pod_bytes: bytes) -> None:
        serialized_pod_id = serialize_pod_id(pod_id)
        self.pod_data.append((serialized_pod_id, pod_bytes))

    def write_dep(self, pod_id: PodId, dep: PodDependency) -> None:
        serialized_pod_id = serialize_pod_id(pod_id)
        new_deps = [(serialized_pod_id, serialize_pod_id(d)) for d in dep.dep_pids]
        self.dependencies += new_deps


class Neo4jPodStorageReader(PodReader):
    def __init__(self, storage: Neo4jPodStorage, hint_pod_ids: List[PodId], cache: Dict[bytes, bytes]) -> None:
        self.storage = storage
        self.hint_pod_ids = hint_pod_ids
        self.cache = cache

    def read(self, pod_id: PodId) -> io.IOBase:
        serialized_pod_id = serialize_pod_id(pod_id)
        if serialized_pod_id in self.cache:
            return io.BytesIO(self.cache[serialized_pod_id])
        with self.storage.session() as session:
            result = session.run("MATCH (p:Pod {pod_id: $pod_id}) RETURN p.pod_bytes", pod_id=serialized_pod_id)
            record = result.single()
        if record is None:
            raise KeyError(f"Data not found for Pod ID: {pod_id}")

        pod_bytes = record["p.pod_bytes"]
        pod_bytes = cast(bytes, pod_bytes)
        return io.BytesIO(pod_bytes)


class Neo4jPodStorage(PodStorage):
    def __init__(self, uri: str, port: int, password: str, database: Optional[str] = None) -> None:
        self.driver = neo4j.GraphDatabase.driver(f"{uri}:{port}", auth=("neo4j", password))
        self.database = database
        with self.session() as session:
            session.run("CREATE INDEX pod_id_index IF NOT EXISTS FOR (p:Pod) ON (p.pod_id);")

    def session(self) -> neo4j.Session:
        if self.database is not None:
            return self.driver.session(database=self.database)
        return self.driver.session()

    def writer(self) -> PodWriter:
        return Neo4jPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        serialized_pod_ids = [serialize_pod_id(pid) for pid in hint_pod_ids]
        cache = {}
        with self.session() as session:
            query = """
            UNWIND $pod_ids AS pod_id
            MATCH (p:Pod {pod_id: pod_id})
            OPTIONAL MATCH (p)-[:DEPENDS_ON*]->(depPod:Pod)
            WITH p, COLLECT(DISTINCT depPod) AS depPods
            UNWIND [p] + depPods AS allPods
            RETURN DISTINCT allPods.pod_id AS pod_id, allPods.pod_bytes AS pod_bytes
            """
            result = session.run(query, pod_ids=serialized_pod_ids)
            for record in result:
                pid = record["pod_id"]
                pod_bytes = record["pod_bytes"]
                if pod_bytes:
                    cache[pid] = pod_bytes

        return Neo4jPodStorageReader(self, hint_pod_ids, cache)

    def estimate_size(self) -> int:
        """Gets size of all files in used neo4j database"""
        home_directory = os.path.expanduser("~")
        search_pattern = os.path.join(home_directory, "neo4j-*/data/databases/neo4j")
        matching_directories = glob.glob(search_pattern)
        if len(matching_directories) > 1:
            raise RuntimeError("Multiple Neo4j installations found. Please make sure only one exists in your user directory")
        elif len(matching_directories) == 0:
            raise RuntimeError("No Neo4j installation found. Please make sure you have it installed in your user directory")
        else:
            neo4j_dir = matching_directories[0]
        neo4j_path = os.path.join(home_directory, neo4j_dir)
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(neo4j_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        return total_size

    def __del__(self):
        self.driver.close()


"""MongoDB Pod Storage"""


class MongoPodStorageWriter(PodWriter):
    MAX_BYTES_SIZE = 15_000_000  # 15 MB

    def __init__(self, storage: MongoPodStorage) -> None:
        self.storage = storage
        self.pods: List[Tuple[SerializedPodId, Optional[bytes]]] = []
        self.dependency_map: Dict[SerializedPodId, Set[PodId]] = {}
        self.new_pid_synonyms: Dict[SerializedPodId, SerializedPodId] = {}
        self.new_ranks: Dict[SerializedPodId, int] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush_data()

    def flush_data(self):
        all_docs = []
        deps_list = []
        for serialized_pod_id, pod_bytes in self.pods:
            is_synonym = serialized_pod_id in self.new_pid_synonyms
            if is_synonym:
                self.storage.synonyms[serialized_pod_id] = self.new_pid_synonyms[serialized_pod_id]
            current_docs = self._construct_pod_documents(serialized_pod_id, pod_bytes, is_synonym)
            deps = self._construct_dependency_documents(
                serialized_pod_id
            )  # Assumes all pod bytes and dependencies are written
            all_docs.extend(current_docs)
            deps_list.append(deps)
        syns_list = self._construct_synonym_documents()
        if all_docs:
            self.storage.db.pod_storage.insert_many(all_docs)
        if deps_list:
            self.storage.db.pod_dependencies.insert_many(deps_list)
        if syns_list:
            self.storage.db.pod_synonyms.insert_many(syns_list)
        self.pods = []
        self.dependency_map = {}
        self.new_pid_synonyms = {}
        self.new_ranks = {}

    def _construct_pod_documents(self, serialized_pod_id: SerializedPodId, pod_bytes: Optional[bytes], is_synonym: bool):
        if not is_synonym:
            assert pod_bytes is not None
            pod_bytes_memview = memoryview(pod_bytes)
            return [
                {
                    "pod_id": serialized_pod_id,
                    "chunk": i,
                    "pod_bytes": pod_bytes_memview[i : i + MongoPodStorageWriter.MAX_BYTES_SIZE].tobytes(),
                    **({"rank": self.new_ranks[serialized_pod_id]} if i == 0 else {}),
                }
                for i in range(0, len(pod_bytes), MongoPodStorageWriter.MAX_BYTES_SIZE)
            ]
        else:
            return [{"pod_id": serialized_pod_id, "rank": self.new_ranks[serialized_pod_id]}]

    def _construct_dependency_documents(self, serialized_pod_id: SerializedPodId):
        deps_list = [
            serialize_pod_id(dep_pid)
            for dep_pid in self.dependency_map[serialized_pod_id]
            if serialized_pod_id in self.dependency_map
        ]
        deps = {"pod_id": serialized_pod_id, "dependencies": deps_list}
        return deps

    def _construct_synonym_documents(self):
        return [
            {"pod_id": serialized_pod_id, "synonym": serialized_synonym}
            for serialized_pod_id, serialized_synonym in self.new_pid_synonyms.items()
        ]

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        serialized_pod_id = serialize_pod_id(pod_id)
        if pod_bytes in self.storage.pod_bytes_memo:
            self.new_pid_synonyms[serialized_pod_id] = serialize_pod_id(self.storage.pod_bytes_memo.get(pod_bytes))
            self.pods.append((serialized_pod_id, None))
        else:
            self.storage.pod_bytes_memo.put(pod_bytes, pod_id)
            self.pods.append((serialized_pod_id, pod_bytes))

    def write_dep(
        self,
        pod_id: PodId,
        dep: PodDependency,  # List of pids this pod depends on.
    ) -> None:
        self.dependency_map[serialize_pod_id(pod_id)] = dep.dep_pids
        self.new_ranks[serialize_pod_id(pod_id)] = dep.rank


class MongoPodStorageReader(PodReader):
    def __init__(
        self,
        storage: MongoPodStorage,
        hint_pod_ids: List[PodId],
        cache: Dict[SerializedPodId, bytearray],
        ranks: Dict[SerializedPodId, int],
    ) -> None:
        self.storage = storage
        self.hint_pod_ids = hint_pod_ids
        self.cache = cache
        self.ranks = ranks

    def read(self, pod_id: PodId) -> io.IOBase:
        serialized_pod_id = serialize_pod_id(pod_id)
        serialized_pod_id = self.storage.synonyms.get(serialized_pod_id, serialized_pod_id)
        if serialized_pod_id not in self.cache:
            cursor = self.storage.db.pod_storage.find({"pod_id": serialized_pod_id}).sort({"chunk": 1})
            pod_byte_array = bytearray()
            for result in cursor:
                if "pod_bytes" in result:
                    pod_byte_array.extend(result["pod_bytes"])
                else:
                    raise ValueError(f"Invalid chunk for pod id: {pod_id}")
            if len(pod_byte_array) == 0:
                raise KeyError(f"Data not found for Pod ID: {pod_id}")
            self.cache[serialized_pod_id] = pod_byte_array
        return io.BytesIO(self.cache[serialized_pod_id])

    def dep_pids_by_rank(self) -> List[PodId]:
        ranked_pod_ids = [deserialize_pod_id(p) for p in self.ranks.keys()]
        ranked_pod_ids.sort(key=lambda p: self.ranks[serialize_pod_id(p)])
        return ranked_pod_ids


class MongoPodStorage(PodStorage):
    def __init__(self, host: str, port: int) -> None:
        self.mongo_client: pymongo.MongoClient = pymongo.MongoClient(host=host, port=port)
        self.db = self.mongo_client.pod
        self.db.pod_storage.create_index("pod_id")
        self.db.pod_dependencies.create_index("pod_id")
        self.db.pod_synonyms.create_index("pod_id")
        self.pod_bytes_memo: PodBytesMemo = PodBytesMemo.new(POD_CACHE_SIZE)
        self.synonyms: Dict[SerializedPodId, SerializedPodId] = {}
        self._fetch_synonyms()

    def _fetch_synonyms(self):
        synonyms = self.db.pod_synonyms.find({})
        for row in synonyms:
            self.synonyms[row["pod_id"]] = row["synonym"]

    def writer(self) -> PodWriter:
        return MongoPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        serialized_hint_pod_ids = [serialize_pod_id(pid) for pid in hint_pod_ids]
        pipeline = [
            {"$match": {"pod_id": {"$in": serialized_hint_pod_ids}}},
            {
                "$graphLookup": {
                    "from": "pod_dependencies",
                    "startWith": "$pod_id",
                    "connectFromField": "dependencies",
                    "connectToField": "pod_id",
                    "as": "all_dependencies",
                }
            },
            {"$project": {"orig_pod_ids": {"$setUnion": [["$pod_id"], "$all_dependencies.pod_id"]}}},
            {
                "$lookup": {
                    "from": "pod_synonyms",
                    "localField": "orig_pod_ids",
                    "foreignField": "pod_id",
                    "as": "synonyms_docs",
                }
            },
            {
                "$project": {
                    "orig_pod_ids": 1,
                    "synonym_pod_ids": "$synonyms_docs.synonym",
                }
            },
            {"$unwind": "$orig_pod_ids"},
            {"$unwind": "$synonym_pod_ids"},
            {
                "$group": {
                    "_id": None,
                    "original_pod_ids": {"$addToSet": "$orig_pod_ids"},
                    "syn_pod_ids": {"$addToSet": "$synonym_pod_ids"},
                }
            },
        ]
        all_pod_ids = self.db.pod_dependencies.aggregate(pipeline, allowDiskUse=True)  # type: ignore
        original_pod_ids = []
        pod_ids_to_fetch = []
        for row in all_pod_ids:
            original_pod_id_row = row.get("original_pod_ids", [])
            original_pod_ids.extend(original_pod_id_row)
            pod_ids_to_fetch.extend(original_pod_id_row)
            synonym_pod_id_row = row.get("syn_pod_ids", [])
            pod_ids_to_fetch.extend(synonym_pod_id_row)

        orig_set = set(original_pod_ids)

        loaded_pod_bytes = {}
        ranks = {}
        result = self.db.pod_storage.find({"pod_id": {"$in": pod_ids_to_fetch}}, {"_id": 0}).sort({"pod_id": 1, "chunk": 1})
        for row in result:
            if "rank" in row and row["pod_id"] in orig_set:
                ranks[row["pod_id"]] = row["rank"]
            if row["pod_id"] not in loaded_pod_bytes:
                loaded_pod_bytes[row["pod_id"]] = bytearray()
            if "pod_bytes" in row:
                loaded_pod_bytes[row["pod_id"]].extend(row["pod_bytes"])
        return MongoPodStorageReader(self, hint_pod_ids, loaded_pod_bytes, ranks)

    def estimate_size(self) -> int:
        pod_storage_stats = self.db.command("collstats", self.db.pod_storage.name)
        pod_dep_stats = self.db.command("collstats", self.db.pod_dependencies.name)
        pod_syns_stats = self.db.command("collstats", self.db.pod_synonyms.name)
        return pod_storage_stats["size"] + pod_dep_stats["size"] + pod_syns_stats["size"]  # Size in bytes
