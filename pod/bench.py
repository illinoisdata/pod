from __future__ import annotations

import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import nbformat
import numpy as np
import simple_parsing
from dataclasses_json import dataclass_json
from loguru import logger

from pod.common import PodId
from pod.pickling import IndividualPodPickling, PodPickling, SnapshotPodPickling
from pod.storage import DictPodStorage, FilePodStorage, PostgreSQLPodStorage

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
    exp1_num_loads: int = 10  # Number of loads to test.

    """ Random mutating list """
    rmlist_num_cells: int = 5  # Number of cells.
    rmlist_list_size: int = 100  # Number of elements in the list
    rmlist_elem_size: int = 10  # Size of each element in the list.
    rmlist_num_elem_mutate: int = 10  # Number of elements mutating in each cell.

    """ Pod storage """
    pod_dir: Optional[Path] = None  # Path to pod storage root directory.


""" Measurement, logging, and output """


def strf_deltatime(time_s: float) -> str:
    if time_s >= 3600.0:
        return f"{time_s // 3600:.0f}:{(time_s % 3600) // 60:.0f}:{time_s % 60:.0f}"
    if time_s >= 60.0:
        return f"{time_s // 60:.0f}:{time_s % 60:.0f}"
    if time_s >= 1.0:
        return f"{time_s:.1f} sec"
    if time_s >= 1e-3:
        return f"{time_s*1e3:.1f} msec"
    return f"{time_s*1e6:.1f} usec"


def strf_storage(storage_b: int) -> str:
    if storage_b >= 1e9:
        return f"{storage_b / 1e9:.1f} GB"
    if storage_b >= 1e6:
        return f"{storage_b / 1e6:.1f} MB"
    if storage_b >= 1e3:
        return f"{storage_b / 1e3:.1f} KB"
    return f"{storage_b} B"


def strf_throughput(tput: float) -> str:
    if tput >= 1e9:
        return f"{tput / 1e9:.1f} Gop/s"
    if tput >= 1e6:
        return f"{tput / 1e6:.1f} Mop/s"
    if tput >= 1e3:
        return f"{tput / 1e3:.1f} Kop/s"
    return f"{tput:.1f} op/s"


@dataclass_json
@dataclass
class DumpStat:
    nth: int
    time_s: float
    storage_b: int


@dataclass_json
@dataclass
class LoadStat:
    nth: int
    time_s: float


@dataclass_json
@dataclass
class ExpStat:
    dumps: List[DumpStat] = field(default_factory=lambda: [])
    loads: List[LoadStat] = field(default_factory=lambda: [])

    dump_sum_t_s: float = 0.0
    load_sum_t_s: float = 0.0

    def add_dump(self, nth: int, time_s: float, storage_b: int) -> None:
        self.dumps.append(
            DumpStat(
                nth=nth,
                time_s=time_s,
                storage_b=storage_b,
            )
        )

        self.dump_sum_t_s += time_s
        dump_avg_t_s = self.dump_sum_t_s / len(self.dumps)
        logger.info(
            f"nth= {nth}, t= {strf_deltatime(time_s)}, s= {strf_storage(storage_b)}"
            f", avgt= {strf_deltatime(dump_avg_t_s)} ({strf_throughput(1.0/dump_avg_t_s)})"
        )

    def add_load(self, nth: int, time_s: float) -> None:
        self.loads.append(
            LoadStat(
                nth=nth,
                time_s=time_s,
            )
        )

        self.load_sum_t_s += time_s
        load_avg_t_s = self.load_sum_t_s / len(self.loads)
        logger.info(
            f"nth= {nth}, t= {strf_deltatime(time_s)}"
            f", avgt= {strf_deltatime(load_avg_t_s)} ({strf_throughput(1.0/load_avg_t_s)})"
        )

    def summary(self) -> None:
        dump_avg_t_s = self.dump_sum_t_s / len(self.dumps)
        load_avg_t_s = self.load_sum_t_s / len(self.loads)
        logger.info(f"{len(self.dumps)} dumps" f", avgt= {strf_deltatime(dump_avg_t_s)} ({strf_throughput(1.0/dump_avg_t_s)})")
        logger.info(f"{len(self.loads)} loads" f", avgt= {strf_deltatime(load_avg_t_s)} ({strf_throughput(1.0/load_avg_t_s)})")

    def save(self, save_dir: Path) -> Path:
        save_dir.mkdir(parents=True, exist_ok=True)
        result_path = save_dir / "expstat.json"
        with open(result_path, "w") as f:
            f.write(self.to_json())  # type: ignore
        return result_path

    @staticmethod
    def load(save_dir: Path) -> ExpStat:
        result_path = save_dir / "expstat.json"
        with open(result_path, "r") as f:
            return ExpStat.from_json(f.read())  # type: ignore


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
                "def f():\n"
                "  return None\n"
                "import random\n"
                "l = [\n"
                f"  random.choices(range(2), k={self.elem_size})\n"
                f"  for _ in range({self.list_size})\n"
                "]"
            )

        # Mutate elements randomly.
        return (
            f"for idx in random.sample(range(len(l)), {self.num_elem_mutate}):\n"
            f"  l[idx] = random.choices(range(2), k={self.elem_size})"
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
            except Exception as e:
                logger.error("Exception while executing...\n" f"{cell}\n" f"...with exception {type(e).__name__}: {e}")
                sys.exit(2)
            yield cell, self.the_globals, self.the_locals


""" Systems under test """


class SUT:
    @staticmethod
    def sut(args: BenchArgs) -> PodPickling:
        if args.sut == "inmem_dict":
            return IndividualPodPickling(DictPodStorage())
        if args.sut == "snapshot":
            assert args.pod_dir is not None, "snapshot requires --pod_dir"
            return SnapshotPodPickling(args.pod_dir)
        if args.sut == "pod_file":
            assert args.pod_dir is not None, "pod_file requires --pod_dir"
            return IndividualPodPickling(FilePodStorage(args.pod_dir))
        elif args.sut == "postgres":
            return IndividualPodPickling(PostgreSQLPodStorage("localhost", 5432))
        raise ValueError(f'Invalid SUT name "{args.sut}"')


""" Main procedures """


def run_exp1(argv: List[str]) -> None:
    """Dumps and loads"""

    # Parse arguments.
    args = simple_parsing.parse(BenchArgs, args=argv)
    random.seed(args.seed)
    np.random.seed(args.seed)
    logger.info(args)

    # Load notebook.
    nb_cells = Notebooks.nb(args)
    nb_exec = NotebookExecutor(nb_cells)

    # Setup storage system under test.
    sut = SUT.sut(args)

    # Dumps all steps.
    expstat = ExpStat()
    pids: List[PodId] = []
    for nth, (cell, the_globals, the_locals) in enumerate(nb_exec.iter()):
        # Dump current state.
        dump_start_ts = time.time()
        pid = sut.dump(the_locals)
        dump_stop_ts = time.time()

        # Record measurements.
        pids.append(pid)
        expstat.add_dump(
            nth=nth,
            time_s=dump_stop_ts - dump_start_ts,
            storage_b=sut.estimate_size(),
        )
    logger.info(f"Collected pids {pids}")

    # Load random steps.
    for nth, idx in enumerate(random.choices(range(len(pids)), k=args.exp1_num_loads)):
        # Load state.
        load_start_ts = time.time()
        _ = sut.load(pids[idx])
        load_stop_ts = time.time()

        # Record measurements.
        expstat.add_load(
            nth=nth,
            time_s=load_stop_ts - load_start_ts,
        )
    expstat.summary()

    # Write results
    result_path = expstat.save(args.result_dir / args.expname)
    logger.info(f"Saved ExpStat to {result_path}")


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
