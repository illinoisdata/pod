from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import contextlib
import gc
import io
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

import pod.storage
from pod._pod import (
    AsyncPodObjectStorage,
    CloudpickleObjectStorage,
    CRIUObjectStorage,
    DillObjectStorage,
    Namespace,
    ObjectStorage,
    PodNamespace,
    PodObjectStorage,
    ShelveObjectStorage,
    SnapshotObjectStorage,
    ZODBObjectStorage,
    ZODBSplitObjectStorage,
)
from pod.bench_consts import PARTIAL_LOAD_NAMES
from pod.common import TimeId
from pod.feature import __FEATURE__
from pod.memo import MemoPageAllocator
from pod.model import (
    ConstRoCModel,
    FeatureCollectorModel,
    GreedyPoddingModel,
    LightGBMClassifierRoC,
    NaiveBundlePoddingModel,
    NaivePoddingModel,
    RandomPoddingModel,
    RoCFeatureCollectorModel,
    XGBRegressorRoC,
)
from pod.pickling import (
    ManualPodding,
    PoddingFunction,
    PodPickling,
    PostPoddingFunction,
    SnapshotPodPickling,
    StaticPodPickling,
    pickle,
)
from pod.static import AllowlistStaticCodeChecker, AlwaysNonStaticCodeChecker, StaticCodeChecker
from pod.stats import ExpStat
from pod.storage import DictPodStorage, FilePodStorage, MongoPodStorage, Neo4jPodStorage, PostgreSQLPodStorage, RedisPodStorage

import pod.__pickle__  # noqa, isort:skip


""" Parameters """

"""Examples:

python pod/bench.py exp1 --expname test --nb rmlist --sut inmem_dict
python pod/bench.py exp1 --expname test --nb rmlist --sut pod_file --pod_dir /tmp/pod
"""


@dataclass
class BenchArgs:
    expname: str  # Experiment name.
    nbname: str  # Notebook name.
    nb: str  # String path to notebook or special notebook names.
    sut: str  # Name of the system under test.

    result_dir: Path = field(default_factory=lambda: Path("./result"))  # Directory to store results.
    seed: int = 123  # Seed for randomization.

    """ Exp1: dumps and loads """
    exp1_num_loads_per_save: int = 4  # Number of loads to test.
    exp1_partial_load: bool = True  # Whether to test partial loading.
    auto_static_checker: str = "allowlist"  # Code check and automatically declare static cells.

    """ Random mutating list """
    rmlist_num_cells: int = 10  # Number of cells.
    rmlist_list_size: int = 1000  # Number of elements in the list
    rmlist_elem_size: int = 100000  # Size of each element in the list.
    rmlist_num_elem_mutate: int = 10  # Number of elements mutating in each cell.

    """ Pod storage """
    sut_async: bool = False  # Use async SUT.
    always_lock_all: bool = False  # Always lock all variables (disabling active variable locks).
    pod_dir: Optional[Path] = None  # Path to pod storage root directory.
    pod_active_filter: bool = True  # Whether to filter active variables for saving.
    pod_cache_size: int = 32_000_000_000  # Pod thesaurus capacity.
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
    const_roc: float = 1.0  # Constant rate of change for const model.

    # Cost model.
    cm_pod_overhead: float = 1200  # Overhead for each pod (bytes per save).
    roc_path: Optional[Path] = None  # Path to rate of change model.


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
                "def f(x):\n"  # Test pickling functions.
                "  def g():\n"
                "    return h()\n"
                "  return g() if x else 0\n"
                "def h():\n"
                "  return f(False)\n"
                f"lc = secrets.token_bytes({self.list_size} * {self.elem_size})\n"
                "l = [\n"
                f"  secrets.token_bytes({self.elem_size})\n"
                f"  for idx in range({self.list_size})\n"
                "]\n"
                "l_share = l + [\n"  # Sharing immutable data with mutating list.
                f"  secrets.token_bytes({self.elem_size})\n"
                f"  for idx in range({self.list_size})\n"
                "]\n"
                "l2 = [l]; l.append(l2)\n"  # Test self-referential objects.
                "a = []; b1 = [a]; b2 = [a]"  # Test shared reference.
            )

        # Mutate elements randomly.
        return (
            f"for idx in random.sample(range(len(l) - 1), {self.num_elem_mutate}):\n"
            f"  l[idx] = secrets.token_bytes({self.elem_size})\n"
            f"a.append({idx})"
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


class CodeCheckers:
    @staticmethod
    def checker(args: BenchArgs) -> StaticCodeChecker:
        if args.auto_static_checker == "always":
            return AlwaysNonStaticCodeChecker()
        elif args.auto_static_checker == "allowlist":
            return AllowlistStaticCodeChecker()
        raise ValueError(f'Invalid code checker name "{args.auto_static_checker}"')


class NotebookExecutor:
    def __init__(self, cells: NotebookCells, checker: StaticCodeChecker, the_globals: dict = {}) -> None:
        self.cells = cells
        self.checker = checker
        self.the_globals: dict = the_globals
        self.the_globals["__spec__"] = None
        self.the_globals["__builtins__"] = globals()["__builtins__"]

    def num_cells(self) -> int:
        return len(self.cells)

    def iter(self) -> Generator[Tuple[NotebookCell, dict, str, str], None, None]:
        for cell in self.cells.iter():
            if isinstance(self.the_globals, PodNamespace):
                self.the_globals.set_managed(False)
            is_static = self.checker.is_static(cell, self.the_globals)
            if isinstance(self.the_globals, PodNamespace):
                self.the_globals.set_managed(True)

            if is_static and isinstance(self.the_globals, PodNamespace):
                # logger.info(f"Found static cell\n{cell}")
                self.the_globals.set_managed(False)
            #     with open("staticlines.txt", "a") as ns:
            #         ns.write(cell + "\n\n_______________\n\n")
            # else:
            #     with open("nonstatic.txt", "a") as ns:
            #         ns.write(cell + "\n\n_______________\n\n")
            stdout, stderr = "", ""
            try:
                with contextlib.redirect_stdout(io.StringIO()) as stdout_f, contextlib.redirect_stderr(
                    io.StringIO()
                ) as stderr_f:
                    exec(cell, self.the_globals, self.the_globals)
                    stdout = stdout_f.getvalue()
                    stderr = stderr_f.getvalue()
            except Exception:
                logger.error("Exception while executing...\n" f"{cell}\n" f"...with {traceback.format_exc()}")
                sys.exit(2)
            if is_static and isinstance(self.the_globals, PodNamespace):
                self.the_globals.set_managed(True)
            yield cell, self.the_globals, stdout, stderr


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
        elif args.podding_model == "naive":
            if args.nbname in ["ai4code"]:
                MemoPageAllocator.PAGE_SIZE = 2**6
                logger.info(f"Tuning down {MemoPageAllocator.PAGE_SIZE=}")
            naive_model = NaivePoddingModel()
            return naive_model.podding_fn, None
        elif args.podding_model == "naive-bundle":
            naive_bundle_model = NaiveBundlePoddingModel()
            return naive_bundle_model.podding_fn, None
        elif args.podding_model == "greedy-xgb":
            assert args.roc_path is not None, "greedy-xgb requires --roc_path"
            roc_xgb_model = XGBRegressorRoC.load_from(args.roc_path)
            gx_model = GreedyPoddingModel(roc_model=roc_xgb_model, pod_overhead=args.cm_pod_overhead)
            return gx_model.podding_fn, gx_model.post_podding_fn
        elif args.podding_model == "greedy-lgb":
            assert args.roc_path is not None, "greedy-lgb requires --roc_path"
            roc_lgb_model = LightGBMClassifierRoC.load_from(args.roc_path)
            gl_model = GreedyPoddingModel(roc_model=roc_lgb_model, pod_overhead=args.cm_pod_overhead)
            return gl_model.podding_fn, gl_model.post_podding_fn
        elif args.podding_model == "greedy-const":
            const_roc_model = ConstRoCModel(args.const_roc)
            gc_model = GreedyPoddingModel(roc_model=const_roc_model, pod_overhead=args.cm_pod_overhead)
            return gc_model.podding_fn, gc_model.post_podding_fn
        elif args.podding_model == "random":
            if args.nbname in ["ai4code", "twittnet"]:
                MemoPageAllocator.PAGE_SIZE = 2**6
                logger.info(f"Tuning down {MemoPageAllocator.PAGE_SIZE=}")
            return RandomPoddingModel().podding_fn, None
        elif args.podding_model == "manual-collect":
            fc_model = FeatureCollectorModel(args.result_dir / args.expname / "manual-collect.csv", ManualPodding.podding_fn)
            return fc_model.podding_fn, fc_model.post_podding_fn
        elif args.podding_model == "roc-collect":
            rocc_model = RoCFeatureCollectorModel(
                args.result_dir / args.expname / "roc-collect" / "feature.csv",
                args.result_dir / args.expname / "roc-collect" / "change.csv",
            )
            return rocc_model.podding_fn, rocc_model.post_podding_fn
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
    def sut(args: BenchArgs) -> ObjectStorage:
        pod.storage.POD_CACHE_SIZE = args.pod_cache_size
        if args.sut == "snapshot":
            pickling = SUT.pickling(args)
            return SnapshotObjectStorage(pickling)
        elif args.sut == "dill":
            assert args.pod_dir is not None, "dill requires --pod_dir"
            return DillObjectStorage(args.pod_dir)
        elif args.sut == "cloudpickle":
            assert args.pod_dir is not None, "cloudpickle requires --pod_dir"
            return CloudpickleObjectStorage(args.pod_dir)
        elif args.sut == "shelve":
            assert args.pod_dir is not None, "shelve requires --pod_dir"
            return ShelveObjectStorage(args.pod_dir)
        elif args.sut == "zodb":
            assert args.pod_dir is not None, "zodb requires --pod_dir"
            return ZODBObjectStorage(args.pod_dir)
        elif args.sut == "zosp":
            assert args.pod_dir is not None, "zosp requires --pod_dir"
            return ZODBSplitObjectStorage(args.pod_dir)
        elif args.sut == "criu":
            assert args.pod_dir is not None, "criu requires --pod_dir"
            return CRIUObjectStorage(args.pod_dir, incremental=False, skip_load=True)
        elif args.sut == "crii":
            assert args.pod_dir is not None, "crii requires --pod_dir"
            return CRIUObjectStorage(args.pod_dir, incremental=True, skip_load=True)
        elif args.sut == "criu-load":
            assert args.pod_dir is not None, "criu-load requires --pod_dir"
            return CRIUObjectStorage(args.pod_dir, incremental=False, skip_save=True)
        elif args.sut == "crii-load":
            assert args.pod_dir is not None, "crii-load requires --pod_dir"
            return CRIUObjectStorage(args.pod_dir, incremental=True, skip_save=True)
        else:  # pod suts.
            pickling = SUT.pickling(args)
            if args.sut_async:
                return AsyncPodObjectStorage(pickling, args.pod_active_filter, args.always_lock_all)
            return PodObjectStorage(pickling, args.pod_active_filter)


""" Main procedures """


def run_exp1_impl(args: BenchArgs) -> None:
    # from scalene import scalene_profiler

    # Setup random state.
    random.seed(args.seed)
    np.random.seed(args.seed)

    # Setup storage system under test.
    sut = SUT.sut(args)

    # Load notebook.
    nb_cells = Notebooks.nb(args)
    checker = CodeCheckers.checker(args)
    namespace = sut.new_managed_namespace()
    nb_exec = NotebookExecutor(nb_cells, checker, the_globals=namespace)
    if args.podding_model == "roc-collect":
        RoCFeatureCollectorModel.NAMESPACE = namespace

    # Load variable names for partial loads.
    if args.nbname in PARTIAL_LOAD_NAMES:
        partial_load_names = PARTIAL_LOAD_NAMES[args.nbname]
        logger.info(f"Using partial load names= {partial_load_names}")
    else:
        partial_load_names = {}
        logger.warning(f"Missing partial load names for nbname= {args.nbname}")

    # Measurement tracker.
    expstat = ExpStat()
    sut.instrument(expstat)

    # Disable GC to reduce noise.
    gc.disable()

    # Dumps all steps.
    tids: List[TimeId] = []
    nb_exec_step = nb_exec.iter()
    exec_start_ts = time.time()
    for nth in range(nb_exec.num_cells()):
        # Execute next cell.
        exec_start_ts = time.time()
        cell, the_globals, stdout, stderr = next(nb_exec_step)
        exec_stop_ts = time.time()

        # Dump current state.
        # scalene_profiler.start()
        dump_start_ts = time.time()
        tid = sut.save(the_globals)
        dump_stop_ts = time.time()
        # scalene_profiler.stop()

        # Record measurements.
        tids.append(tid)
        expstat.add_exec_time(exec_stop_ts - exec_start_ts)
        expstat.add_dump(
            nth=nth,
            time_s=dump_stop_ts - dump_start_ts,
            storage_b=sut.estimate_size(),
        )

    # Wait until all background dumps are done.
    dump_start_ts = time.time()
    sut.join()
    dump_stop_ts = time.time()
    expstat.add_dump(
        nth=nth + 1,
        time_s=dump_stop_ts - dump_start_ts,
        storage_b=sut.estimate_size(),
    )

    # Collect once before test loading.
    gc.collect()

    # Early summary
    expstat.summary()
    logger.info(f"Collected tids {tids}")

    # Save partial results (in case of load failure).
    (args.result_dir / args.expname).mkdir(parents=True, exist_ok=True)
    result_path = expstat.save(args.result_dir / args.expname / "expstat.json")
    logger.info(f"Saved ExpStat (dump only) to {result_path}")

    # Reset the random state.
    random.seed(args.seed)
    np.random.seed(args.seed)

    # Test equal number of loads per time ID.
    test_tids = tids * args.exp1_num_loads_per_save
    random.shuffle(test_tids)
    logger.info(f"Testing {len(test_tids)} loads, {test_tids}")

    # Load random steps.
    loaded_globals: Optional[Namespace] = None
    for nth, tid in enumerate(test_tids):
        # Get load variable names.
        if args.exp1_partial_load:
            load_set = partial_load_names.get(tid, None)
            if load_set is None:
                logger.warning(f"Missing partial load names for nbname= {args.nbname}, tid= {tid}")
        else:
            load_set = None

        # Load state.
        load_start_ts = time.time()
        try:
            with BlockTimeout(600):
                loaded_globals = sut.load(tid, nameset=load_set)
        except TimeoutError as e:
            logger.warning(f"{e}")
        load_stop_ts = time.time()

        # Share reference test.
        # loaded_globals["a"].append("NEW")
        # print(loaded_globals["a"], loaded_globals["b1"], loaded_globals["b2"])

        try:
            loaded_size_b = len(pickle.dumps(loaded_globals))
        except Exception:
            loaded_size_b = 0

        # Record measurements.
        expstat.add_load(
            nth=nth,
            tid=tid,
            time_s=load_stop_ts - load_start_ts,
            load_b=loaded_size_b,
        )

        # Reset environment to reduce noise.
        if loaded_globals is not None:
            del loaded_globals
            loaded_globals = None
        gc.collect()
        time.sleep(1.0)
    expstat.summary()

    # No more measurement, re-enable now.
    gc.enable()

    # Write results
    result_path = expstat.save(args.result_dir / args.expname / "expstat.json")
    logger.info(f"Saved ExpStat to {result_path}")


def run_exp1(argv: List[str]) -> None:
    # Parse arguments.
    args = simple_parsing.parse(BenchArgs, args=argv)
    logger.info(args)
    logger.info(f"Using base pickle= {pod.__pickle__.BASE_PICKLE}")

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
