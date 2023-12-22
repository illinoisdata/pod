"""
Key-value storages with correlated/poset reads
"""

from __future__ import annotations

import io
import pickle
import psycopg2
from pathlib import Path
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


class FilePodStorageWriter(PodWriter):
    def __init__(self, storage: FilePodStorage) -> None:
        self.storage = storage

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        # TODO: Reduce fragments by combining and flushing in pages.
        with open(self.storage.pod_path(pod_id), "wb") as f:
            f.write(pod_bytes)

    def write_dep(
        self,
        pod_id: PodId,
        dep_pids: Set[PodId],  # List of pids this pod depends on.
    ) -> None:
        with open(self.storage.dep_path(pod_id), "wb") as f:
            pickle.dump(dep_pids, f)


class FilePodStorageReader(PodReader):
    def __init__(self, storage: FilePodStorage) -> None:
        self.storage = storage

    def read(self, pod_id: PodId) -> io.IOBase:
        return open(self.storage.pod_path(pod_id), "rb")


class FilePodStorage(PodStorage):
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def writer(self) -> PodWriter:
        return FilePodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        # TODO: Leverage dep to prefetch relevant pods.
        return FilePodStorageReader(self)

    def pod_path(self, pid: PodId) -> Path:
        return self.root_dir / f"{pid.tid}_{pid.oid}_pod.pkl"

    def dep_path(self, pid: PodId) -> Path:
        return self.root_dir / f"{pid.tid}_{pid.oid}_dep.pkl"

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
        with self.db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT pg_total_relation_size('pod_dependencies') + pg_total_relation_size('pod_storage') 
                           AS total_size;
            """)
            size = cursor.fetchone()[0]
        return size

    
    def __del__(self):
        self.db_conn.close()