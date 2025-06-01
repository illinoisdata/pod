from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import contextlib
import gc
import inspect
import io
import random
import signal
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, List, Optional, Set, Tuple, cast

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
    ExhaustivePodObjectStorage,
    ExperimentNamespace,
    Namespace,
    NoopObjectStorage,
    ObjectStorage,
    PodObjectStorage,
    ShelveObjectStorage,
    SkipSavingPodObjectStorage,
    SnapshotObjectStorage,
    ZODBObjectStorage,
    ZODBSplitObjectStorage,
)
from pod.bench_consts import EXCLUDED_SAVE_NAMES, PARTIAL_LOAD_NAMES
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
    CompressedSnapshotPodPickling,
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
from pod.storage import (
    CompressedFilePodStorage,
    DictPodStorage,
    FilePodStorage,
    MongoPodStorage,
    Neo4jPodStorage,
    PostgreSQLPodStorage,
    RedisPodStorage,
)

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
    exp1_num_loads_per_save: int = 1  # Number of loads to test.
    exp1_partial_load: bool = True  # Whether to test partial loading.
    auto_static_checker: str = "allowlist"  # Code check and automatically declare static cells.
    exclude_save_names: bool = False  # Whether to exclude selected variable names.

    """ Random mutating list """
    rmlist_num_cells: int = 10  # Number of cells.
    rmlist_list_size: int = 1000  # Number of elements in the list.
    rmlist_elem_size: int = 100000  # Size of each element in the list.
    rmlist_num_elem_mutate: int = 10  # Number of elements mutating in each cell.

    """ Random mutating tree """
    rmtree_num_cells: int = 10  # Number of cells.
    rmtree_var_size: float = 1000  # Number of list variables.
    rmtree_list_size: float = 1000  # Number of elements in the list.
    rmtree_elem_size: int = 100  # Size of each element in the list.
    rmtree_percent_list_mutate: float = 10  # Percentage of list variables mutating in each cell.
    rmtree_percent_elem_mutate: float = 10  # Percentage of elements mutating in each cell.

    """ Pod storage """
    sut_async: bool = False  # Use async SUT.
    sut_compress: bool = False  # Compress bytes.
    always_lock_all: bool = False  # Always lock all variables (disabling active variable locks).
    pod_dir: Optional[Path] = None  # Path to pod storage root directory.
    pod_active_filter: bool = True  # Whether to filter active variables for saving.
    pod_cache_size: int = 32_000_000_000  # Pod thesaurus capacity.
    pod_noop: bool = False  # Skip saving to measure any execution overhead.
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
    decision_count: Optional[int] = None  # Number of decisions to be made (exhaustive podding).

    """ Learning, model, feature """
    podding_model: str = "manual"  # Model name to use for podding function.
    enable_feature: bool = False  # Whether to enable feature extraction
    const_roc: float = 1.0  # Constant rate of change for const model.

    # Cost model.
    cm_pod_overhead: float = 1200  # Overhead for each pod (bytes per save).
    roc_path: Optional[Path] = None  # Path to rate of change model.


""" Python handler/executor """


NotebookCell = str


class ExperimentExecutor:
    def iter(self) -> Generator[Tuple[NotebookCell, ExperimentNamespace, str, str], None, None]:
        # Returns last executed code, current namespace, stdout, stderr
        raise NotImplementedError("Abstract method")


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


class ScriptCells(NotebookCells):
    def __init__(self, notebook_path: Path) -> None:
        # Read notebook.
        with open(notebook_path, "r") as f:
            source = "".join(f.readlines())

        # TODO: Parse and separate out statements (~cells) via AST.
        self.cells = [source]

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


class RandomMutatingTreeCells(NotebookCells):
    NBFORMAT_VERSION = 4

    def __init__(
        self,
        num_cells: int,
        var_size: int,
        list_size: int,
        elem_size: int,
        percent_list_mutate: float,
        percent_elem_mutate: float,
    ) -> None:
        self.num_cells = num_cells
        self.var_size = var_size
        self.list_size = list_size
        self.elem_size = elem_size
        self.num_list_mutate = int(var_size * percent_list_mutate / 100)
        self.num_elem_mutate = int(list_size * percent_elem_mutate / 100)

    def __getitem__(self, idx: int, /) -> NotebookCell:
        if idx == 0:
            # First cell, declare an empty list.
            return (
                "import secrets\n"
                "import random\n"
                "random.seed(73)\n"
                f"for idx in range({self.var_size}):\n"
                "  globals()[f'l_{idx}'] = [\n"
                f"    secrets.token_bytes({self.elem_size})\n"
                f"    for idx in range({self.list_size})\n"
                "  ]"
            )

        # Mutate elements randomly.
        return (
            f"for idx in random.sample(range({self.var_size}), {self.num_list_mutate}):\n"
            f"  for jdx in random.sample(range({self.list_size}), {self.num_elem_mutate}):\n"
            "    globals()[f'l_{idx}'][jdx] = (\n"
            f"      secrets.token_bytes({self.elem_size})\n"
            "    )"
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
        elif args.nb == "rmtree":
            return RandomMutatingTreeCells(
                args.rmtree_num_cells,
                int(args.rmtree_var_size),
                int(args.rmtree_list_size),
                args.rmtree_elem_size,
                args.rmtree_percent_list_mutate,
                args.rmtree_percent_elem_mutate,
            )
        else:
            source_path = Path(args.nb)
            if source_path.suffix == ".ipynb":
                return FileNotebookCells(source_path)
            elif source_path.suffix == ".py":
                return ScriptCells(source_path)
            raise ValueError(f"Target file has an invalid source extension {source_path}")


class CodeCheckers:
    @staticmethod
    def checker(args: BenchArgs) -> StaticCodeChecker:
        if args.auto_static_checker == "always":
            return AlwaysNonStaticCodeChecker()
        elif args.auto_static_checker == "allowlist":
            return AllowlistStaticCodeChecker()
        raise ValueError(f'Invalid code checker name "{args.auto_static_checker}"')


class NotebookExecutor(ExperimentExecutor):
    def __init__(self, cells: NotebookCells, checker: StaticCodeChecker, the_globals: dict = {}) -> None:
        ExperimentExecutor.__init__(self)
        self.cells = cells
        self.checker = checker
        self.the_globals = cast(ExperimentNamespace, the_globals)
        self.the_globals["__spec__"] = None
        self.the_globals["__builtins__"] = globals()["__builtins__"]

    def iter(self) -> Generator[Tuple[NotebookCell, ExperimentNamespace, str, str], None, None]:
        for cell in self.cells.iter():
            with self.the_globals.set_managed(False):
                is_static = self.checker.is_static(cell, self.the_globals)
                # if is_static:
                #     logger.info(f"Found static cell\n{cell}")
                #     with open("staticlines.txt", "a") as ns:
                #         ns.write(cell + "\n\n_______________\n\n")
                # else:
                #     with open("nonstatic.txt", "a") as ns:
                #         ns.write(cell + "\n\n_______________\n\n")

            stdout, stderr = "", ""
            try:
                with (
                    contextlib.redirect_stdout(io.StringIO()) as stdout_f,
                    contextlib.redirect_stderr(io.StringIO()) as stderr_f,
                    self.the_globals.set_managed(not is_static),
                ):
                    exec(cell, self.the_globals, self.the_globals)
                    stdout = stdout_f.getvalue()
                    stderr = stderr_f.getvalue()
            except Exception:
                logger.error("Exception while executing...\n" f"{cell}\n" f"...with {traceback.format_exc()}")
                sys.exit(2)
            yield cell, self.the_globals, stdout, stderr


class PodSaveFunction:
    def __init__(self, executor: Optional[ScriptExecutor]) -> None:
        self.executor = executor

    def __call__(self) -> None:  # __pod_save__
        assert self.executor is not None

        # Extract namespaces.
        raw_frame = inspect.currentframe()
        assert raw_frame is not None
        raw_frame = raw_frame.f_back  # Step up once to skip __pod_save__ frame.
        save_scopes: List[dict] = []
        while raw_frame is not None and id(raw_frame) != self.executor._exec_frame_id:
            save_scopes.append(raw_frame.f_locals)
            raw_frame = raw_frame.f_back
        save_scopes = save_scopes[::-1]  # Flip so the first scope is the global.

        # HACK: Combine into the first namespace.
        global_scope = save_scopes[0]
        assert isinstance(global_scope, ExperimentNamespace)
        for key in list(global_scope.keys()):
            if key.startswith("stack::"):
                del global_scope[key]
        for stack_idx, local_scope in enumerate(save_scopes[1:]):
            global_scope[f"stack::{stack_idx}"] = local_scope

        # Yields and unblock.
        self.executor._save_yield = global_scope
        self.executor._save_yield_event.set()
        self.executor._save_continue_event.wait()

        # Now back from parent and ready to continue
        self.executor._save_continue_event.clear()

    def __reduce__(self):
        # Skip saving and loading this class.
        return lambda x: x, (None,)


class ScriptExecutor(ExperimentExecutor):
    def __init__(self, cells: NotebookCells, checker: StaticCodeChecker, sut: ObjectStorage) -> None:
        ExperimentExecutor.__init__(self)
        self.cells = cells
        self.checker = checker
        self.sut = sut
        self.__pod_save__ = PodSaveFunction(self)

        self.the_globals = self.sut.new_managed_namespace()
        self.the_globals["__spec__"] = None
        self.the_globals["__builtins__"] = globals()["__builtins__"]
        self.the_globals["__pod_save__"] = self.__pod_save__

        self._save_continue_event = threading.Event()
        self._save_yield_event = threading.Event()
        self._save_yield: Optional[ExperimentNamespace] = None
        self._save_exception: Optional[Exception] = None

        self._exec_frame_id: Optional[int] = None  # To be set at the beginning of execution.

        # To be set while executing.
        self._current_cell: Optional[str] = None
        self._current_stdout_f: Optional[io.StringIO] = None
        self._current_stderr_f: Optional[io.StringIO] = None

    def iter(self) -> Generator[Tuple[NotebookCell, ExperimentNamespace, str, str], None, None]:
        assert self._exec_frame_id is None, "ScriptExecutor can be iterated once."

        # Start running the execution in a separate thread.
        exec_thread = threading.Thread(target=self._run)
        exec_thread.start()
        while exec_thread.is_alive():
            try:
                global_scope = self._wait_yield()
                if global_scope is not None:
                    with (
                        contextlib.redirect_stdout(sys.stdout),
                        contextlib.redirect_stderr(sys.stderr),
                    ):
                        assert isinstance(self._current_cell, str)
                        assert isinstance(self._current_stdout_f, io.StringIO)
                        assert isinstance(self._current_stderr_f, io.StringIO)
                        stdout = self._current_stdout_f.getvalue()
                        stderr = self._current_stderr_f.getvalue()
                        del self.the_globals["__pod_save__"]
                        yield self._current_cell, global_scope, stdout, stderr
                        self.the_globals["__pod_save__"] = self.__pod_save__
                elif self._save_exception is not None:
                    sys.exit(2)
            finally:
                self._continue_yield()

    def _run(self) -> None:
        try:
            # Register this frame to truncate from saving.
            self._exec_frame_id = id(sys._getframe())

            # Run cell one by one.
            for idx, cell in enumerate(self.cells.iter()):
                # Preprocess cell.
                self._current_cell = cell
                with self.the_globals.set_managed(False):
                    is_static = self.checker.is_static(cell, self.the_globals)

                # Execute the cell.
                try:
                    with (
                        contextlib.redirect_stdout(io.StringIO()) as stdout_f,
                        contextlib.redirect_stderr(io.StringIO()) as stderr_f,
                        self.the_globals.set_managed(not is_static),
                    ):
                        self._current_stdout_f = stdout_f
                        self._current_stderr_f = stderr_f
                        exec(cell, self.the_globals, self.the_globals)
                except Exception as e:
                    logger.error("Exception while executing...\n" f"{cell}\n" f"...with {traceback.format_exc()}")
                    self._save_exception = e
                    break
        finally:
            # Last yield.
            self._save_yield = None
            self._save_yield_event.set()

    def _wait_yield(self) -> Optional[ExperimentNamespace]:
        self._save_yield_event.wait()
        return self._save_yield

    def _continue_yield(self) -> None:
        self._save_yield_event.clear()
        self._save_continue_event.set()


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


class SelectNamespace:
    def __init__(self, namespace: ExperimentNamespace, exclude_names: Set[str] = set()):
        self.namespace = namespace
        self.exclude_names = exclude_names
        self.excluded_items: dict = {}

    def __enter__(self):
        with self.namespace.set_managed(False):
            for exclude_name in self.exclude_names:
                if exclude_name in self.namespace:
                    self.excluded_items[exclude_name] = self.namespace.pop(exclude_name)

    def __exit__(self, type, value, traceback):
        with self.namespace.set_managed(False):
            self.namespace.update(self.excluded_items)


def make_executor(args: BenchArgs, sut: ObjectStorage) -> ExperimentExecutor:
    nb_cells = Notebooks.nb(args)
    checker = CodeCheckers.checker(args)
    namespace = sut.new_managed_namespace()
    if args.podding_model == "roc-collect":
        RoCFeatureCollectorModel.NAMESPACE = namespace
    if isinstance(nb_cells, ScriptCells):
        return ScriptExecutor(nb_cells, checker, sut)
    else:
        return NotebookExecutor(nb_cells, checker, the_globals=namespace)


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
                args.result_dir / args.expname / "roc-collect" / "dep.csv",
                args.result_dir / args.expname / "roc-collect" / "var.csv",
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
        elif args.sut == "snapshotzlib":
            assert args.pod_dir is not None, "snapshot requires --pod_dir"
            return CompressedSnapshotPodPickling(args.pod_dir, delta=False)
        elif args.sut == "snapshotxdelta":
            assert args.pod_dir is not None, "snapshot requires --pod_dir"
            return CompressedSnapshotPodPickling(args.pod_dir, delta=True)
        elif args.sut == "pod_file":
            assert args.pod_dir is not None, "pod_file requires --pod_dir"
            if args.sut_compress:
                return StaticPodPickling(
                    CompressedFilePodStorage(args.pod_dir), podding_fn=podding_fn, post_podding_fn=post_podding_fn
                )
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
        if args.sut in ["snapshot", "snapshotzlib", "snapshotxdelta"]:
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
            return CRIUObjectStorage(args.pod_dir, incremental=False)
        elif args.sut == "crii":
            assert args.pod_dir is not None, "crii requires --pod_dir"
            return CRIUObjectStorage(args.pod_dir, incremental=True)
        elif args.sut == "noop":
            return NoopObjectStorage()
        elif args.sut == "exhaust":
            if args.decision_count is None and args.nb == "rmtree" and args.exclude_save_names:
                args.decision_count = int(args.rmtree_num_cells * args.rmtree_var_size * (args.rmtree_list_size + 1))
                logger.info(f"Set decision_count= {args.decision_count}")
            assert args.decision_count is not None, "exhaust requires --decision_count"
            assert args.pod_dir is not None, "exhaust requires --pod_dir"
            return ExhaustivePodObjectStorage.make_for(args.decision_count, args.pod_dir)
        else:  # pod suts.
            pickling = SUT.pickling(args)
            if args.pod_noop:
                return SkipSavingPodObjectStorage(pickling, args.pod_active_filter, args.always_lock_all)
            elif args.sut_async:
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

    # Load notebook and executor.
    nb_exec = make_executor(args, sut)

    # Filter out excluded variable names.
    excluded_save_names: Set[str] = set()
    if args.exclude_save_names:
        if args.nbname in EXCLUDED_SAVE_NAMES:
            excluded_save_names = EXCLUDED_SAVE_NAMES[args.nbname]
            logger.info(f"Using excluded save names= {excluded_save_names}")
        else:
            raise ValueError(f"Missing excluded save names for nbname= {args.nbname}")

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
    nth = -1
    while True:
        # Execute next cell.
        nth += 1
        exec_start_ts = time.time()
        try:
            cell, the_globals, stdout, stderr = next(nb_exec_step)
        except StopIteration:
            break
        exec_stop_ts = time.time()

        # Dump current state.
        with SelectNamespace(the_globals, exclude_names=excluded_save_names):
            # scalene_profiler.start()
            dump_start_ts = time.time()
            tid = sut.save(the_globals)
            dump_stop_ts = time.time()
            # scalene_profiler.stop()

        # Record measurements.
        storage_b = sut.estimate_size()
        tids.append(tid)
        expstat.add_exec_time(exec_stop_ts - exec_start_ts)
        expstat.add_dump(
            nth=nth,
            time_s=dump_stop_ts - dump_start_ts,
            storage_b=storage_b,
        )
        if storage_b > 768e9:  # 768 GB
            logger.error(f"Storage limit exceeded: {storage_b} bytes")
            break

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
        time.sleep(1.0)

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
            with BlockTimeout(2400):
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
