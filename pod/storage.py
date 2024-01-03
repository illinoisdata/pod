"""
Key-value storages with correlated/poset reads
"""

from __future__ import annotations

import glob
import io
import os
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, cast

import psycopg2
import redis
from dataclasses_json import dataclass_json
from neo4j import GraphDatabase

from pod.common import PodId


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
        self.storage_buffer: List[Tuple] = []
        self.dependency_buffer: List[Tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.storage.db_conn.rollback()
        else:
            self.flush_storage()
            self.flush_dependencies()
            self.storage.db_conn.commit()

    def flush_storage(self):
        with self.storage.db_conn.cursor() as cursor:
            values_str = ",".join(cursor.mogrify("(%s, %s, %s)", x).decode() for x in self.storage_buffer)
            query = f"""
                INSERT INTO pod_storage (tid, oid, pod_bytes)
                VALUES {values_str}
                ON CONFLICT (tid, oid) DO UPDATE SET pod_bytes = EXCLUDED.pod_bytes;
            """
            cursor.execute(query)
        self.storage_buffer = []

    def flush_dependencies(self):
        with self.storage.db_conn.cursor() as cursor:
            # Constructing insert query
            insert_values = ",".join(cursor.mogrify("(%s,%s,%s,%s)", x).decode() for x in self.dependency_buffer)
            if insert_values:
                insert_query = f"""
                    INSERT INTO pod_dependencies (pod_id_tid, pod_id_oid, dep_pid_tid, dep_pid_oid)
                    VALUES {insert_values}
                    ON CONFLICT DO NOTHING
                """
                cursor.execute(insert_query)
        self.dependency_buffer = []

    def write_pod(
        self,
        pod_id: PodId,
        pod_bytes: bytes,
    ) -> None:
        self.storage_buffer.append((pod_id.tid, pod_id.oid, pod_bytes))

    def write_dep(
        self,
        pod_id: PodId,
        dep_pids: Set[PodId],  # List of pids this pod depends on.
    ) -> None:
        self.dependency_buffer += [(pod_id.tid, pod_id.oid, p.tid, p.oid) for p in dep_pids]


class PostgreSQLPodStorageReader(PodReader):
    def __init__(self, storage: PostgreSQLPodStorage) -> None:
        self.storage = storage

    def read(self, pod_id: PodId) -> io.IOBase:
        if (pod_id.tid, pod_id.oid) in self.storage.cache:
            return self.storage.cache[(pod_id.tid, pod_id.oid)]
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
        self.cache: Dict[Tuple, io.BytesIO] = {}
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
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
            """
            )
            self.db_conn.commit()

    def writer(self) -> PodWriter:
        return PostgreSQLPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        hint_tid_oid_tup = tuple([(p.tid, p.oid) for p in hint_pod_ids])
        with self.db_conn.cursor() as cursor:
            query = """
            WITH RECURSIVE dependency_chain AS (
                SELECT pd.dep_pid_tid AS tid, pd.dep_pid_oid AS oid
                FROM pod_dependencies pd
                WHERE (pd.pod_id_tid, pd.pod_id_oid) IN %s
                UNION ALL
                SELECT pd.dep_pid_tid, pd.dep_pid_oid
                FROM pod_dependencies pd
                INNER JOIN dependency_chain dc ON pd.pod_id_tid = dc.tid AND pd.pod_id_oid = dc.oid
            )
            SELECT ps.tid, ps.oid, ps.pod_bytes
            FROM (
                SELECT tid, oid FROM dependency_chain
                UNION
                SELECT tid, oid FROM pod_storage WHERE (tid, oid) IN %s
            ) AS combined
            INNER JOIN pod_storage ps ON combined.tid = ps.tid AND combined.oid = ps.oid;
            """
            cursor.execute(query, (hint_tid_oid_tup, hint_tid_oid_tup))
            results = cursor.fetchall()
            for row in results:
                tid, oid, pod_bytes = row
                self.cache[(tid, oid)] = io.BytesIO(pod_bytes)
        return PostgreSQLPodStorageReader(self)

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
        self.db_conn.close()


""" Redis storage """


class RedisPodStorageWriter(PodWriter):
    def __init__(self, storage: RedisPodStorage) -> None:
        self.storage = storage
        self.pod_data: Dict[bytes, bytes] = {}
        self.dependency_map: Dict[bytes, Set[bytes]] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush_data()

    def flush_data(self):
        with self.storage.redis_client.pipeline() as pipe:
            for serialized_pod_id, bytes in self.pod_data.items():
                pipe.set(f"pod_bytes:{serialized_pod_id}", bytes)
                if serialized_pod_id in self.dependency_map:
                    serialized_dep_pids = self.dependency_map[serialized_pod_id]
                    if serialized_dep_pids:
                        pipe.sadd(f"dep_pids:{serialized_pod_id}", *serialized_dep_pids)
            pipe.execute()
        self.dependency_map = {}

    def write_pod(self, pod_id: PodId, pod_bytes: bytes) -> None:
        serialized_pod_id = serialize_pod_id(pod_id)
        self.pod_data[serialized_pod_id] = pod_bytes

    def write_dep(self, pod_id: PodId, dep_pids: Set[PodId]) -> None:
        serialized_pod_id = serialize_pod_id(pod_id)
        serialized_dep_pids = {serialize_pod_id(pid) for pid in dep_pids}
        self.dependency_map[serialized_pod_id] = serialized_dep_pids


class RedisPodStorageReader(PodReader):
    def __init__(self, storage: RedisPodStorage) -> None:
        self.storage = storage

    def read(self, pod_id: PodId) -> io.IOBase:
        serialized_pod_id = serialize_pod_id(pod_id)
        pod_bytes = self.storage.redis_client.get(f"pod_bytes:{serialized_pod_id!r}")
        pod_bytes = cast(bytes, pod_bytes)
        if pod_bytes is None:
            raise KeyError(f"Data not found for Pod ID: {pod_id}")
        return io.BytesIO(pod_bytes)


class RedisPodStorage(PodStorage):
    def __init__(self, host: str, port: int) -> None:
        self.redis_client = redis.Redis(host=host, port=port)

    def writer(self) -> PodWriter:
        return RedisPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        return RedisPodStorageReader(self)

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
        with self.storage.driver.session() as session:
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

    def write_dep(self, pod_id: PodId, dep_pids: Set[PodId]) -> None:
        serialized_pod_id = serialize_pod_id(pod_id)
        new_deps = [(serialized_pod_id, serialize_pod_id(dep)) for dep in dep_pids]
        self.dependencies += new_deps


class Neo4jPodStorageReader(PodReader):
    def __init__(self, storage: Neo4jPodStorage) -> None:
        self.storage = storage

    def read(self, pod_id: PodId) -> io.IOBase:
        serialized_pod_id = serialize_pod_id(pod_id)
        if serialized_pod_id in self.storage.cache:
            return self.storage.cache[serialized_pod_id]
        with self.storage.driver.session() as session:
            result = session.run("MATCH (p:Pod {pod_id: $pod_id}) RETURN p.pod_bytes", pod_id=serialized_pod_id)
            record = result.single()
        if record is None:
            raise KeyError(f"Data not found for Pod ID: {pod_id}")

        pod_bytes = record["p.pod_bytes"]
        pod_bytes = cast(bytes, pod_bytes)
        return io.BytesIO(pod_bytes)


class Neo4jPodStorage(PodStorage):
    def __init__(self, uri: str, port: int) -> None:
        self.driver = GraphDatabase.driver(f"{uri}:{port}", auth=("neo4j", "pod_neo4j"))
        with self.driver.session() as session:
            session.run("CREATE INDEX pod_id_index IF NOT EXISTS FOR (p:Pod) ON (p.pod_id);")
        self.cache: Dict[bytes, io.BytesIO] = {}

    def writer(self) -> PodWriter:
        return Neo4jPodStorageWriter(self)

    def reader(self, hint_pod_ids: List[PodId] = []) -> PodReader:
        serialized_pod_ids = [serialize_pod_id(pid) for pid in hint_pod_ids]
        with self.driver.session() as session:
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
                    self.cache[pid] = io.BytesIO(pod_bytes)

        return Neo4jPodStorageReader(self)

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
