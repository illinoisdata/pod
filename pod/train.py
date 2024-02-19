from pod.bench import Notebooks, NotebookExecutor, BenchArgs
from pod.pickling import StaticPodPickling
from pod.storage import FilePodStorage
from model import QLearningPoddingModel
from pod.stats import ExpStat
from pod.feature import __FEATURE__
from typing import List
import time
from pathlib import Path
from pod.common import PodId
from loguru import logger
import gc
import sys
import numpy as np
import shutil
import os
from dataclasses import dataclass
import simple_parsing
from tqdm import tqdm
from multiprocessing import Process, Queue

@dataclass
class TrainArgs:
    gamma: float
    alpha: float


# def train_notebook_iter(nb_path, q):

def train(n_epochs, nbs, args: TrainArgs):
    """Trains model on n_epochs on nbs"""
    eps = 1.0
    eps_decay_factor = 0.9
    try:
        os.mkdir("qtables")
    except FileExistsError:
        pass
    with __FEATURE__:
        save_file_str = (str(args.gamma) + "&" + str(args.alpha)).replace(".", "-")
        model = QLearningPoddingModel(train=True, gamma=args.gamma, alpha=args.alpha)
        for n in tqdm(range(n_epochs)):
            model.set_epsilon(eps)
            eps *= eps_decay_factor
            for nb_path in nbs:

                args = BenchArgs(expname="", nb=nb_path, sut="inmem_dict")
                # Load notebook.
                nb_cells = Notebooks.nb(args=args)
                nb_exec = NotebookExecutor(nb_cells)

                pod_storage_path = Path(f"tmp/pod{save_file_str}")
                if pod_storage_path.exists():
                    shutil.rmtree(pod_storage_path)

                # Initialize sut
                sut = StaticPodPickling(FilePodStorage(Path(f"tmp/pod{save_file_str}")), podding_fn=model.podding_fn, post_podding_fn=model.post_podding_fn)
                
                last_storage_size = 0
                expstat = ExpStat()
                pids: List[PodId] = []
                for nth, (cell, the_globals, the_locals) in enumerate(nb_exec.iter()):
                    # Dump current state.
                    dump_start_ts = time.time()
                    pid = sut.dump(the_locals)
                    dump_stop_ts = time.time()

                    # Record measurements.
                    cur_size = sut.estimate_size()
                    pids.append(pid)
                    expstat.add_dump(
                        nth=nth,
                        time_s=dump_stop_ts - dump_start_ts,
                        storage_b=cur_size,
                    )

                    # Rudimentary reward function based on dump time/storage size, this can be changed to include load time as well
                    size = cur_size - last_storage_size
                    dump_time = dump_stop_ts - dump_start_ts
                    reward = -1000*dump_time + -1*size
                    last_storage_size = cur_size

                    # Reset environment to reduce noise.
                    gc.collect()

                    # Updates Q values based on actions made for dumps
                    model.batch_update_q(reward=reward)

                if "simple" not in nb_path:
                    model.size_history.append(size)
                    model.dump_time_history.append(dump_time)
                logger.info(f"Collected pids {pids}")
                expstat.summary()
                unique_vals,counts = np.unique(model.q_table, return_counts=True)
                model.nnz_qtable.append(counts[-1])

        # model.plot_stats(name=save_file_str)
        # model.save_q_table(f"qtables/{save_file_str}.npy")

if __name__ == "__main__":
    # logger.info(f"Arguments {sys.argv}")
    if len(sys.argv) != 5:
        logger.error("Usage --gamma <gamma> --alpha <alpha>")
        sys.exit(2)
    args = simple_parsing.parse(TrainArgs, args=sys.argv[1:])
    # train(1, ["notebooks/simple.ipynb", "notebooks/04_training_linear_models.ipynb"], args) - goods
    train(1, ["notebooks/twitter_networks.ipynb"], args)
    train(1, ["notebooks/amex-dataset.ipynb"], args)
    train(1, ["notebooks/better-xgb-baseline.ipynb"], args)
    train(1, ["notebooks/it-s-that-time-of-the-year-again.ipynb"], args)
    # train(1, [ "notebooks/sklearn_tweet_classification.ipynb"], args)
   