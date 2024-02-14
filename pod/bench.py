from __future__ import annotations

import gc
import random
import signal
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import nbformat
import numpy as np
import simple_parsing
from loguru import logger

from pod._pod import Pod
from pod.common import TimeId
from pod.feature import __FEATURE__
from pod.model import FeatureCollectorModel, RandomPoddingModel
from pod.pickling import (
    ManualPodding,
    PoddingFunction,
    PodPickling,
    PostPoddingFunction,
    SnapshotPodPickling,
    StaticPodPickling,
)
from pod.stats import ExpStat
from pod.storage import DictPodStorage, FilePodStorage, MongoPodStorage, Neo4jPodStorage, PostgreSQLPodStorage, RedisPodStorage

""" Parameters """

"""Examples:

python pod/bench.py exp1 --expname test --nb rmlist --sut inmem_dict
python pod/bench.py exp1 --expname test --nb rmlist --sut pod_file --pod_dir /tmp/pod
"""


@dataclass
class BenchArgs:
    expname: str  # Experiment name.
    nb: str  # String path to notebook or special notebook names.
    sut: str  # Name of the system under test.

    result_dir: Path = field(default_factory=lambda: Path("./result"))  # Directory to store results.
    seed: int = 123  # Seed for randomization.

    """ Exp1: dumps and loads """
    exp1_num_loads: int = 100  # Number of loads to test.

    """ Random mutating list """
    rmlist_num_cells: int = 10  # Number of cells.
    rmlist_list_size: int = 1000  # Number of elements in the list
    rmlist_elem_size: int = 100000  # Size of each element in the list.
    rmlist_num_elem_mutate: int = 10  # Number of elements mutating in each cell.

    """ Pod storage """
    pod_dir: Optional[Path] = None  # Path to pod storage root directory.
    psql_hostname: str = "localhost"  # Hostname where PostgreSQL server is running.
    psql_port: int = 5432  # Port on the hostname where PostgreSQL server is running.
    redis_hostname: str = "localhost"  # Hostname where Redis server is running.
    redis_port: int = 6379  # Port on the hostname where Redis server is running.
    neo4j_uri: str = "neo4j://localhost"  # URI where Neo4j server is running.
    neo4j_port: int = 7687  # Port on the hostname where Neo4j server is running.
    neo4j_password: str = "pod_neo4j"  # Password to access the Neo4j server.
    neo4j_database: Optional[str] = None  # Database name to store pod data.
    mongo_hostname: str = "localhost"  # Hostname where MongoDB server is running.
    mongo_port: int = 27017  # Port on the hostname where MongoDB server is running.

    """ Learning, model, feature """
    podding_model: str = "manual"  # Model name to use for podding function.
    enable_feature: bool = False  # Whether to enable feature extraction


""" Notebook handler/executor """


NotebookCell = str


class NotebookCells:
    def __getitem__(self, idx: int, /) -> NotebookCell:
        raise NotImplementedError("Abstract method")

    def __len__(self) -> int:
        raise NotImplementedError("Abstract method")

    def iter(self) -> Generator[NotebookCell, None, None]:
        for idx in range(len(self)):
            yield self[idx]


class FileNotebookCells(NotebookCells):
    NBFORMAT_VERSION = 4

    def __init__(self, notebook_path: Path) -> None:
        # Read notebook.
        with open(notebook_path, "r") as f:
            nb = nbformat.read(f, FileNotebookCells.NBFORMAT_VERSION)

        # Extract only code cells.
        self.cells = [cell.source for cell in nb.cells if cell.cell_type == "code"]

    def __getitem__(self, idx: int, /) -> NotebookCell:
        return self.cells[idx]

    def __len__(self) -> int:
        return len(self.cells)


class RandomMutatingListCells(NotebookCells):
    NBFORMAT_VERSION = 4

    def __init__(
        self,
        num_cells: int,
        list_size: int,
        elem_size: int,
        num_elem_mutate: int,
    ) -> None:
        self.num_cells = num_cells
        self.list_size = list_size
        self.elem_size = elem_size
        self.num_elem_mutate = num_elem_mutate

    def __getitem__(self, idx: int, /) -> NotebookCell:
        if idx == 0:
            # First cell, declare an empty list.
            return (
                "import secrets\n"
                "import random\n"
                "def f():\n"  # Test pickling functions.
                "  def g():\n"
                "    return 0\n"
                "  return 0\n"
                "l = [\n"
                f"  secrets.token_bytes({self.elem_size})\n"
                f"  for idx in range({self.list_size})\n"
                "]\n"
                "l2 = [l]; l.append(l2)"  # Test self-referential objects.
            )

        # Mutate elements randomly.
        return (
            f"for idx in random.sample(range(len(l)), {self.num_elem_mutate}):\n"
            f"  l[idx] = secrets.token_bytes({self.elem_size})"
        )

    def __len__(self) -> int:
        return self.num_cells


class Notebooks:
    @staticmethod
    def nb(args: BenchArgs) -> NotebookCells:
        if args.nb == "rmlist":
            return RandomMutatingListCells(
                args.rmlist_num_cells,
                args.rmlist_list_size,
                args.rmlist_elem_size,
                args.rmlist_num_elem_mutate,
            )
        else:
            notebook_path = Path(args.nb)
            return FileNotebookCells(notebook_path)


class NotebookExecutor:
    def __init__(self, cells: NotebookCells) -> None:
        self.cells = cells
        self.the_locals: dict = {}
        self.the_globals: dict = self.the_locals
        self.the_globals["__spec__"] = None
        self.the_globals["__builtins__"] = globals()["__builtins__"]

    def iter(self) -> Generator[Tuple[NotebookCell, dict, dict], None, None]:
        for cell in self.cells.iter():
            try:
                exec(cell, self.the_globals, self.the_locals)
            except Exception:
                logger.error("Exception while executing...\n" f"{cell}\n" f"...with {traceback.format_exc()}")
                sys.exit(2)
            yield cell, self.the_globals, self.the_locals


class BlockTimeout:
    def __init__(self, seconds: int, error_message: str = "Timeout"):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(f"Timeout ({self.seconds} seconds)")

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


""" Systems under test """


class SUT:
    @staticmethod
    def podding_model(args: BenchArgs) -> Tuple[PoddingFunction, Optional[PostPoddingFunction]]:
        if args.podding_model == "manual":
            return ManualPodding.podding_fn, None
        elif args.podding_model == "random":
            return RandomPoddingModel().podding_fn, None
        elif args.podding_model == "manual-collect":
            model = FeatureCollectorModel(args.result_dir / args.expname / "manual-collect.csv", ManualPodding.podding_fn)
            return model.podding_fn, model.post_podding_fn
        raise ValueError(f'Invalid model name "{args.podding_model}"')

    @staticmethod
    def pickling(args: BenchArgs) -> PodPickling:
        podding_fn, post_podding_fn = SUT.podding_model(args)
        if args.sut == "inmem_dict":
            return StaticPodPickling(DictPodStorage(), podding_fn=podding_fn, post_podding_fn=post_podding_fn)
        elif args.sut == "snapshot":
            assert args.pod_dir is not None, "snapshot requires --pod_dir"
            return SnapshotPodPickling(args.pod_dir)
        elif args.sut == "pod_file":
            assert args.pod_dir is not None, "pod_file requires --pod_dir"
            return StaticPodPickling(FilePodStorage(args.pod_dir), podding_fn=podding_fn, post_podding_fn=post_podding_fn)
        elif args.sut == "pod_psql":
            return StaticPodPickling(
                PostgreSQLPodStorage(args.psql_hostname, args.psql_port),
                podding_fn=podding_fn,
                post_podding_fn=post_podding_fn,
            )
        elif args.sut == "pod_redis":
            return StaticPodPickling(
                RedisPodStorage(args.redis_hostname, args.redis_port), podding_fn=podding_fn, post_podding_fn=post_podding_fn
            )
        elif args.sut == "pod_neo4j":
            return StaticPodPickling(
                Neo4jPodStorage(
                    args.neo4j_uri,
                    args.neo4j_port,
                    args.neo4j_password,
                    database=args.neo4j_database,
                ),
                podding_fn=podding_fn,
                post_podding_fn=post_podding_fn,
            )
        elif args.sut == "pod_mongo":
            return StaticPodPickling(
                MongoPodStorage(args.mongo_hostname, args.mongo_port), podding_fn=podding_fn, post_podding_fn=post_podding_fn
            )
        raise ValueError(f'Invalid SUT name "{args.sut}"')

    @staticmethod
    def sut(args: BenchArgs) -> Pod:
        pickling = SUT.pickling(args)
        return Pod(pickling)


""" Main procedures """


def run_exp1_impl(args: BenchArgs) -> None:
    # Setup random state.
    random.seed(args.seed)
    np.random.seed(args.seed)

    # Load notebook.
    nb_cells = Notebooks.nb(args)
    nb_exec = NotebookExecutor(nb_cells)

    # Setup storage system under test.
    sut = SUT.sut(args)

    # Dumps all steps.
    expstat = ExpStat()
    tids: List[TimeId] = []
    for nth, (cell, the_globals, the_locals) in enumerate(nb_exec.iter()):
        # Dump current state.
        dump_start_ts = time.time()
        tid = sut.save(the_locals)
        dump_stop_ts = time.time()

        # Record measurements.
        tids.append(tid)
        expstat.add_dump(
            nth=nth,
            time_s=dump_stop_ts - dump_start_ts,
            storage_b=sut.estimate_size(),
        )

        # Reset environment to reduce noise.
        gc.collect()
    logger.info(f"Collected tids {tids}")

    # Save partial results (in case of load failure).
    result_path = expstat.save(args.result_dir / args.expname)
    logger.info(f"Saved ExpStat (dump only) to {result_path}")

    # Load random steps.
    for nth, idx in enumerate(random.choices(range(len(tids)), k=args.exp1_num_loads)):
        # Load state.
        load_start_ts = time.time()
        try:
            with BlockTimeout(60):
                the_locals = sut.load(tids[idx])
        except TimeoutError as e:
            logger.warning(f"{e}")
        load_stop_ts = time.time()

        # Record measurements.
        expstat.add_load(
            nth=nth,
            time_s=load_stop_ts - load_start_ts,
        )

        # Reset environment to reduce noise.
        del the_locals
        gc.collect()
    expstat.summary()

    # Write results
    result_path = expstat.save(args.result_dir / args.expname)
    logger.info(f"Saved ExpStat to {result_path}")


def run_exp1(argv: List[str]) -> None:
    # Parse arguments.
    args = simple_parsing.parse(BenchArgs, args=argv)
    logger.info(args)

    # Setup learning if needed.
    if args.enable_feature:
        with __FEATURE__:
            run_exp1_impl(args)
    else:
        run_exp1_impl(args)


if __name__ == "__main__":
    logger.info(f"Arguments {sys.argv}")
    if len(sys.argv) < 2:
        logger.error("Missing experiment name, e.g., python pod/bench.py exp1")
        sys.exit(1)

    if sys.argv[1] == "exp1":
        run_exp1(sys.argv[2:])
    else:
        logger.error(f'Unknown experiment "{sys.argv[1]}"')
        sys.exit(1)
