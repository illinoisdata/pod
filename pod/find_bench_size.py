import contextlib
import gc
import io
import multiprocessing as mp
import os
import shutil
import sys
import time
from dataclasses import dataclass
from functools import partial
from multiprocessing import Process, Queue
from pathlib import Path
from typing import List
import json

import numpy as np
from loguru import logger
from model import QLearningPoddingModel
from tqdm import tqdm

from pod.bench import BenchArgs, NotebookExecutor, Notebooks
from pod.common import PodId
from pod.feature import __FEATURE__
from pod.pickling import ManualPodding, SnapshotPodPickling, StaticPodPickling
from pod.stats import ExpStat
from pod.storage import FilePodStorage


@dataclass
class TrainArgs:
    gamma: float
    alpha: float


def run_iter(nb_path, update_q: Queue):
    # print(nb_path)
    args = BenchArgs(expname="", nb=nb_path, sut="snapshot")
    # Load notebook.
    logger.info(f"PID {os.getpid()}, {nb_path}")
    save_file_str = nb_path
    nb_cells = Notebooks.nb(args=args)
    nb_exec = NotebookExecutor(nb_cells)

    pod_storage_path = Path(f"tmp/pod{save_file_str}")
    if pod_storage_path.exists():
        shutil.rmtree(pod_storage_path)

    # Initialize sut
    # sut = SnapshotPodPickling(Path(f"tmp/pod{save_file_str}"))
    sut = SnapshotPodPickling(Path(f"tmp/pod{save_file_str}"))

    sizes = []
    times = []
    last_storage_size = 0
    # expstat = ExpStat()
    pids: List[PodId] = []
    for nth, (cell, the_globals, the_locals) in enumerate(nb_exec.iter()):
        # Dump current state.
        dump_start_ts = time.time()
        pid = sut.dump(the_locals)
        dump_stop_ts = time.time()

        # Record measurements.
        cur_size = sut.estimate_size()
        dump_time = dump_stop_ts - dump_start_ts
        times.append(dump_time)
        pids.append(pid)
        size = cur_size - last_storage_size
        last_storage_size = cur_size
        sizes.append(size)
        # Reset environment to reduce noise.
        gc.collect()

    update_q.put({"nb": nb_path, "sizes" : sizes, "times" : times, "final_size" : cur_size})
    print("DONE")
    return


def find_bench_size(nbs):
    """Finds average size using snapshot"""
    procs: List[Process] = []
    update_q = Queue()
    for nb_path in nbs:
        p = Process(target=run_iter, args=(nb_path, update_q))
        procs.append(p)
        try:
            print("STARTING PROC")
            p.start()
        except:
            logger.info("ERROR STARTING PROCESS")
            return

    global_data = {}
    popped = 0
    while popped < len(nbs):
        print("GETTING FROM UPD")
        data = update_q.get()
        popped += 1
        global_data[data["nb"]] = {"sizes" : data["sizes"], "times" : data["times"], "final_size" : data["final_size"]}
    for p in procs:
        try:
            p.join()
        except:
            logger.info("ERROR JOINING")
    return global_data


if __name__ == "__main__":
    # logger.info(f"Arguments {sys.argv}")
    bench_data = find_bench_size(
        [
            "notebooks/it-s-that-time-of-the-year-again.ipynb",
            "notebooks/better-xgb-baseline.ipynb",
            "notebooks/fast-fourier-transform-denoising.ipynb",
            "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
            # "notebooks/amex-dataset.ipynb",
            "notebooks/denoising-with-direct-wavelet-transform.ipynb",
            "notebooks/04_training_linear_models.ipynb",
        ]
    )
    json_object = json.dumps(bench_data, indent=4)
    with open("benchdata.json", "w") as f:
        print("WRITING")
        f.write(json_object)
