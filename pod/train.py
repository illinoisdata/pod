from pod.bench import Notebooks, NotebookExecutor, BenchArgs
from pod.pickling import StaticPodPickling
from pod.storage import FilePodStorage
from model import QLearningPoddingModel
from pod.stats import ExpStat
from pod.feature import __FEATURE__
from typing import List
from functools import partial
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
import multiprocessing as mp
from multiprocessing import Process, Queue
import contextlib
import io

@dataclass
class TrainArgs:
    gamma: float
    alpha: float


def train_notebook_iter(nb_path, update_q: Queue, train_args: TrainArgs, model: QLearningPoddingModel, p_id: int = 0):
    # print(nb_path)
    args = BenchArgs(expname="", nb=nb_path, sut="pod_file")
    # Load notebook.
    save_file_str = (str(train_args.gamma) + "&" + str(train_args.alpha)).replace(".", "-") + str(p_id)
    nb_cells = Notebooks.nb(args=args)
    with contextlib.redirect_stdout(io.StringIO()) as stdout_f, \
             contextlib.redirect_stderr(io.StringIO()) as stderr_f:
        nb_exec = NotebookExecutor(nb_cells)

        pod_storage_path = Path(f"tmp/pod{save_file_str}")
        if pod_storage_path.exists():
            shutil.rmtree(pod_storage_path)

        hist_list = []
        reward_list = []

        podding_wrapper = partial(model.podding_fn, history_list=hist_list)
        # Initialize sut
        sut = StaticPodPickling(FilePodStorage(Path(f"tmp/pod{save_file_str}")), podding_fn=podding_wrapper, post_podding_fn=model.post_podding_fn)
        
        last_storage_size = 0
        # expstat = ExpStat()
        pids: List[PodId] = []
        reward_sum = 0
        for nth, (cell, the_globals, the_locals) in enumerate(nb_exec.iter()):
            # Dump current state.
            dump_start_ts = time.time()
            pid = sut.dump(the_locals)
            dump_stop_ts = time.time()

            # Record measurements.
            cur_size = sut.estimate_size()
            pids.append(pid)

            size = cur_size - last_storage_size
            dump_time = dump_stop_ts - dump_start_ts
            reward = -1000*dump_time + -1*size
            reward_sum += reward
            last_storage_size = cur_size

            # Reset environment to reduce noise.
            gc.collect()
            reward_list.append(reward)
            hist_list.append("|")
        
        # Generating output to be placed on queue
        
        final_hist_list = []
        curr_hist_list = []
        for item in hist_list:
            if item != "|":
                curr_hist_list.append(item)
            else:
                final_hist_list.append(curr_hist_list)
                curr_hist_list = []
        reward_hist_list = [(reward_list[i], final_hist_list[i]) for i in range(len(final_hist_list))]
        update_q.put((reward_hist_list, cur_size, reward_sum))
        # print("DONE")
    return
         


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
            print(f"EPOCH {n}")
            model.set_epsilon(eps)
            eps *= eps_decay_factor
            procs: List[Process] = []
            update_q = Queue()
            p_id = 0
            for nb_path in nbs:
                p = Process(target=train_notebook_iter, args=(nb_path, update_q, args, model, p_id))
                procs.append(p)
                p.start()
                p_id += 1

            # print("DONE PROCESSES")
            # sys.stdout.flush()
            sizes = []
            rewards = []
            popped = 0
            while popped < len(nbs):
                # print("REMOVING FROM UPDATE")
                update_val, size, reward_sum = update_q.get()
                popped += 1
                model.batch_update_q_parallel(update_val)
                sizes.append(size)
                rewards.append(reward_sum)
            # print("DONE UPD")

            for p in procs:
                # print("JOINING")
                p.join()
            
            model.size_history.append(sum(sizes)/len(sizes))
            model.reward_history.append(sum(rewards)/len(rewards))
        print("PLOTTING")
        model.plot_stats(name=save_file_str)
        print("DONE PLOTTING")
        model.save_q_table(f"qtables/{save_file_str}.npy")

if __name__ == "__main__":
    # logger.info(f"Arguments {sys.argv}")
    if len(sys.argv) != 5:
        logger.error("Usage --gamma <gamma> --alpha <alpha>")
        sys.exit(2)
    args = simple_parsing.parse(TrainArgs, args=sys.argv[1:])
    train(100, [
        "notebooks/simple.ipynb", 
        "notebooks/it-s-that-time-of-the-year-again.ipynb",
        "notebooks/better-xgb-baseline.ipynb",
        "notebooks/fast-fourier-transform-denoising.ipynb",
        "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
        # "notebooks/amex-dataset.ipynb",
        "notebooks/denoising-with-direct-wavelet-transform.ipynb",
        "notebooks/numpy.ipynb",
        "notebooks/04_training_linear_models.ipynb"
    ], args)
    # train(1, ["notebooks/simple.ipynb", "notebooks/04_training_linear_models.ipynb"], args) - good, < 5 min
    # train(1, ["notebooks/twitter_networks.ipynb"], args),  11 min
    # train(1, ["notebooks/amex-dataset.ipynb"], args) < 5 min
    # train(1, ["notebooks/better-xgb-baseline.ipynb"], args) good 5 min
    # train(1, ["notebooks/it-s-that-time-of-the-year-again.ipynb"], args) < 5 min, very sim to 2nd place ncaaw
    # "notebooks/2nd-place-ncaaw-2021.ipynb" good < 5 min
    # "notebooks/fast-fourier-transform-denoising.ipynb" good < 5 min
    # "notebooks/denoising-with-direct-wavelet-transform.ipynb" good < 5 min
    # "notebooks/ncaa-starter-the-simpler-the-better.ipynb" good < 5 min
    # baysean needs ffmpeg notebooks/introduction-to-bayesian-inference.ipynb good < 5 min
    # notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb good < 5 min
    # notebooks/numpy.ipynb good < 5 min