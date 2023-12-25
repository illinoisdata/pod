"""
Key-value storages with correlated/poset reads
"""

from __future__ import annotations

import io
import pickle
from dataclasses import dataclass
import psycopg2
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
    pod_page_count: int
    dep_page_count: int
    pid_synonym_page_count: int


FilePodStorageIndex = Dict[PodId, int]
FilePodStoragePodIdSynonym = Dict[PodId, PodId]
FilePodStoragePodIdByBytes = Dict[bytes, PodId]
FilePodStoragePodPage = Dict[PodId, bytes]
FilePodStorageDepPage = Dict[PodId, Set[PodId]]


class FilePodStorageWriter(PodWriter):
    FLUSH_SIZE = 1_000_000  # 1 MB

    def __init__(self, storage: FilePodStorage) -> None:
        self.storage = storage
        self.new_pid_synonyms: Dict[PodId, PodId] = {}
        self.pod_page_buffer: FilePodStoragePodPage = {}
        self.dep_page_buffer: FilePodStorageDepPage = {}
        self.pod_page_buffer_size: int = 0

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        if pod_bytes in self.storage.pid_by_bytes:
            # Save as synonymous pids.
            same_pod_id = self.storage.pid_by_bytes[pod_bytes]
            self.new_pid_synonyms[pod_id] = same_pod_id
        else:
            # New pod bytes.
            self.storage.pid_by_bytes[pod_bytes] = pod_id

            # Write to buffer.
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
        page_idx = self.storage.next_pod_page_idx()
        with open(self.storage.pod_page_path(page_idx), "wb") as f:
            pickle.dump(self.pod_page_buffer, f)

        # Update index.
        self.storage.update_index({pid: page_idx for pid in self.pod_page_buffer})

        # Reset buffer state.
        self.pod_page_buffer = {}
        self.pod_page_buffer_size = 0

    def flush_dep(self) -> None:
        # WRite buffer.
        page_idx = self.storage.next_dep_page_idx()
        with open(self.storage.dep_page_path(page_idx), "wb") as f:
            pickle.dump(self.dep_page_buffer, f)

        # Reset buffer state.
        self.dep_page_buffer = {}

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if len(self.new_pid_synonyms) > 0:
            self.storage.update_pid_synonym(self.new_pid_synonyms)
        if len(self.pod_page_buffer) > 0:
            self.flush_pod()
        if len(self.dep_page_buffer) > 0:
            self.flush_dep()


class FilePodStorageReader(PodReader):
    def __init__(self, storage: FilePodStorage) -> None:
        self.storage = storage
        self.page_cache: Dict[int, FilePodStoragePodPage] = {}

    def read(self, pod_id: PodId) -> io.IOBase:
        resolved_pid = self.storage.resolve_pid_synonym(pod_id)
        page_idx = self.storage.search_index(resolved_pid)
        page_path = self.storage.pod_page_path(page_idx)
        if page_idx not in self.page_cache:
            with open(page_path, "rb") as f:
                self.page_cache[page_idx] = pickle.load(f)
        page = self.page_cache[page_idx]
        if resolved_pid not in page:
            raise ValueError(f"False index pointing {pod_id} ){resolved_pid}) to {page_path}: {page}")
        return io.BytesIO(page[resolved_pid])


class FilePodStorage(PodStorage):
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)

        self.stats = FilePodStorageStats(
            pod_page_count=0,
            dep_page_count=0,
            pid_synonym_page_count=0,
        )
        self.pid_index: FilePodStorageIndex = {}
        self.pid_synonym: FilePodStoragePodIdSynonym = {}
        self.pid_by_bytes: FilePodStoragePodIdByBytes = {}  # TODO: Move out of memor.
        if self.is_init():
            self.stats = self.reload_stats()
            self.pid_index = self.reload_index()
            self.pid_synonym = self.reload_pid_synonym()
            self.pid_by_bytes = self.reload_pid_by_bytes()  # TODO: Reload as needed.

    def writer(self) -> PodWriter:
        return FilePodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        # TODO: Leverage dep to prefetch relevant pods.
        return FilePodStorageReader(self)

    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self.root_dir.glob("**/*") if f.is_file())

    def dep_path(self, pid: PodId) -> Path:
        return self.root_dir / f"{pid.tid}_{pid.oid}_dep.pkl"

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

    def index_path(self) -> Path:
        return self.root_dir / "index.pkl"

    def pid_synonym_path(self, page_idx: int) -> Path:
        return self.root_dir / f"pid_synonym_{page_idx}.pkl"

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

    def update_pid_synonym(self, new_pid_synonyms: FilePodStoragePodIdSynonym) -> None:
        self.pid_synonym.update(new_pid_synonyms)
        self.write_pid_synonym(new_pid_synonyms)

    def resolve_pid_synonym(self, pid: PodId) -> PodId:
        return self.pid_synonym.get(pid, pid)

    def write_pid_synonym(self, pid_synonym: FilePodStoragePodIdSynonym) -> None:
        page_idx = self.next_pid_synonym_page_idx()
        with open(self.pid_synonym_path(page_idx), "wb") as f:
            pickle.dump(pid_synonym, f)

    def reload_pid_synonym(self) -> FilePodStoragePodIdSynonym:
        pid_synonym: FilePodStoragePodIdSynonym = {}
        for page_idx in range(self.stats.pid_synonym_page_count):
            with open(self.pid_synonym_path(page_idx), "rb") as f:
                pid_synonym.update(pickle.load(f))
        return pid_synonym

    def reload_pid_by_bytes(self) -> FilePodStoragePodIdByBytes:
        pid_by_bytes: FilePodStoragePodIdByBytes = {}
        for page_path in self.root_dir.glob("pod_*.pkl"):
            with open(page_path, "rb") as f:
                page = pickle.load(f)
            for pod_id, pod_bytes in page.items():
                pid_by_bytes[pod_bytes] = pod_id
        return pid_by_bytes

    def is_init(self) -> bool:
        return self.stats_path().exists() and self.index_path().exists()


""" PostgreSQL storage: each pod as an entry in a database """


class PostgreSQLPodStorageWriter(PodWriter):
    def __init__(self, storage: PostgreSQLPodStorage) -> None:
        self.storage = storage

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        # TODO: Reduce fragments by combining and flushing in pages.
        with self.storage.db_conn.cursor() as cursor:
            cursor.execute("INSERT INTO pod_storage (tid, oid, pod_bytes) VALUES (%s, %s, %s) ON CONFLICT (tid, oid) DO UPDATE SET pod_bytes = EXCLUDED.pod_bytes", (pod_id.tid, pod_id.oid, pod_bytes))
        self.storage.db_conn.commit()

    def write_dep(
        self,
        pod_id: PodId,
        dep_pids: Set[PodId],  # List of pids this pod depends on.
    ) -> None:
        with self.storage.db_conn.cursor() as cursor:
            cursor.execute("DELETE FROM pod_dependencies WHERE pod_id_tid = %s AND pod_id_oid = %s;", (pod_id.tid, pod_id.oid))
            for dep_pid in dep_pids:
                cursor.execute("INSERT INTO pod_dependencies (pod_id_tid, pod_id_oid, dep_pid_tid, dep_pid_oid) VALUES (%s, %s, %s, %s);", (pod_id.tid, pod_id.oid, dep_pid.tid, dep_pid.oid))
        self.storage.db_conn.commit()

class PostgreSQLPodStorageReader(PodReader):
    def __init__(self, storage: PostgreSQLPodStorage) -> None:
        self.storage = storage

    def read(self, pod_id: PodId) -> io.IOBase:
        with self.storage.db_conn.cursor() as cursor:
            cursor.execute("SELECT pod_bytes FROM pod_storage WHERE tid = %s AND oid = %s", (pod_id.tid, pod_id.oid))
            pod_bytes = cursor.fetchone()[0]
        return io.BytesIO(pod_bytes)


class PostgreSQLPodStorage(PodStorage):
    def __init__(self, host: str, port: int) -> None:
        try:
            self.db_conn = psycopg2.connect(dbname="postgres", user="postgres", host=host, port=port)
        except psycopg2.OperationalError as e:
            print(f"Error connecting to the database: {e}")
            raise
        with self.db_conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pod_storage (
                    tid BIGINT,
                    oid BIGINT,
                    pod_bytes BYTEA,
                    PRIMARY KEY (tid, oid)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pod_dependencies (
                    pod_id_tid BIGINT,
                    pod_id_oid BIGINT,
                    dep_pid_tid BIGINT,
                    dep_pid_oid BIGINT,
                    PRIMARY KEY (pod_id_tid, pod_id_oid, dep_pid_tid, dep_pid_oid),
                    FOREIGN KEY (pod_id_tid, pod_id_oid) REFERENCES pod_storage(tid, oid),
                    FOREIGN KEY (dep_pid_tid, dep_pid_oid) REFERENCES pod_storage(tid, oid)      
                );
            """)
            self.db_conn.commit()


    def writer(self) -> PodWriter:
        return PostgreSQLPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        # TODO: Leverage dep to prefetch relevant pods.
        return PostgreSQLPodStorageReader(self)
    
    def estimate_size(self) -> int:
        return sum(f.stat().st_size for f in self.root_dir.glob("**/*") if f.is_file())


""" PostgreSQL storage: each pod as an entry in a database """


class PostgreSQLPodStorageWriter(PodWriter):
    def __init__(self, storage: PostgreSQLPodStorage) -> None:
        self.storage = storage

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        # TODO: Reduce fragments by combining and flushing in pages.
        with self.storage.db_conn.cursor() as cursor:
            cursor.execute("INSERT INTO pod_storage (tid, oid, pod_bytes) VALUES (%s, %s, %s) ON CONFLICT (tid, oid) DO UPDATE SET pod_bytes = EXCLUDED.pod_bytes", (pod_id.tid, pod_id.oid, pod_bytes))
        self.storage.db_conn.commit()

    def write_dep(
        self,
        pod_id: PodId,
        dep_pids: Set[PodId],  # List of pids this pod depends on.
    ) -> None:
        with self.storage.db_conn.cursor() as cursor:
            # DELETE operation
            delete_query = "DELETE FROM pod_dependencies WHERE pod_id_tid = %s AND pod_id_oid = %s"

            # Preparing values for INSERT operation
            values = [(pod_id.tid, pod_id.oid, dep_pid.tid, dep_pid.oid) for dep_pid in dep_pids]
            if values:
                args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s)", x).decode("utf-8") for x in values)
                insert_query = "INSERT INTO pod_dependencies (pod_id_tid, pod_id_oid, dep_pid_tid, dep_pid_oid) VALUES " + args_str

                # Combining DELETE and INSERT into a single query using CTE
                combined_query = f"WITH deleted AS ({delete_query}) {insert_query}"

                # Execute the combined query
                cursor.execute(combined_query, (pod_id.tid, pod_id.oid))

        self.storage.db_conn.commit()


class PostgreSQLPodStorageReader(PodReader):
    def __init__(self, storage: PostgreSQLPodStorage) -> None:
        self.storage = storage

    def read(self, pod_id: PodId) -> io.IOBase:
        with self.storage.db_conn.cursor() as cursor:
            cursor.execute("SELECT pod_bytes FROM pod_storage WHERE tid = %s AND oid = %s", (pod_id.tid, pod_id.oid))
            result = cursor.fetchone()
            if result is None:
                raise ValueError("No data found for the given pod_id")
            pod_bytes = result[0]
        return io.BytesIO(pod_bytes)


class PostgreSQLPodStorage(PodStorage):
    def __init__(self, host: str, port: int) -> None:
        try:
            self.db_conn = psycopg2.connect(dbname="postgres", user="postgres", host=host, port=port)
        except psycopg2.OperationalError as e:
            print(f"Error connecting to the database: {e}")
            raise
        with self.db_conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pod_storage (
                    tid BIGINT,
                    oid BIGINT,
                    pod_bytes BYTEA,
                    PRIMARY KEY (tid, oid)
                );
                CREATE TABLE IF NOT EXISTS pod_dependencies (
                    pod_id_tid BIGINT,
                    pod_id_oid BIGINT,
                    dep_pid_tid BIGINT,
                    dep_pid_oid BIGINT,
                    PRIMARY KEY (pod_id_tid, pod_id_oid, dep_pid_tid, dep_pid_oid),
                    FOREIGN KEY (pod_id_tid, pod_id_oid) REFERENCES pod_storage(tid, oid),
                    FOREIGN KEY (dep_pid_tid, dep_pid_oid) REFERENCES pod_storage(tid, oid)      
                );
            """)
            self.db_conn.commit()


    def writer(self) -> PodWriter:
        return PostgreSQLPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        # TODO: Leverage dep to prefetch relevant pods.
        return PostgreSQLPodStorageReader(self)
    
    def estimate_size(self) -> int:
        with self.db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT pg_total_relation_size('pod_dependencies') + pg_total_relation_size('pod_storage') 
                           AS total_size;
            """)
            size = cursor.fetchone()[0]
        return size

    
    def __del__(self):
        self.db_conn.close()