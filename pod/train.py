import gc
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

import simple_parsing
from loguru import logger
from model import QLearningPoddingModel
from tqdm import tqdm

from pod.bench import BenchArgs, NotebookExecutor, Notebooks
from pod.common import PodId
from pod.feature import __FEATURE__
from pod.pickling import ManualPodding, StaticPodPickling
from pod.storage import FilePodStorage
from pod.xgb_predictor import XGBPredictor

with open("benchdata.json", "r") as bench_file:
    BENCH_DATA = json.load(bench_file)


@dataclass
class TrainArgs:
    gamma: float
    alpha: float


def train_notebook_iter(nb_path, update_q: Queue, train_args: TrainArgs, model: QLearningPoddingModel, p_id: int = 0):
    # print(nb_path)
    args = BenchArgs(expname="", nb=nb_path, sut="pod_file")
    # Load notebook.
    logger.info(f"PID {os.getpid()}, {nb_path}")
    save_file_str = (str(train_args.gamma) + "&" + str(train_args.alpha)).replace(".", "-") + str(p_id)
    nb_cells = Notebooks.nb(args=args)
    nb_exec = NotebookExecutor(nb_cells)

    bench_sizes = BENCH_DATA[nb_path]["sizes"]
    bench_times = BENCH_DATA[nb_path]["times"]

    pod_storage_path = Path(f"tmp/pod{save_file_str}")
    if pod_storage_path.exists():
        shutil.rmtree(pod_storage_path)

    hist_list = []
    reward_list = []

    podding_wrapper = partial(model.podding_fn, history_list=hist_list)
    # Initialize sut
    sut = StaticPodPickling(
        FilePodStorage(Path(f"tmp/pod{save_file_str}")), podding_fn=podding_wrapper, post_podding_fn=model.post_podding_fn
    )

    last_storage_size = 0
    pids: List[PodId] = []
    reward_sum = 0
    times = []
    sz = []
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
        times.append(dump_time)
        bench_size = bench_sizes[nth]
        bench_time = bench_times[nth]
        reward = (0.01 * (bench_time - dump_time) / dump_time) + (100 * (bench_size - size) / bench_size)
        sz.append((bench_size - size) / bench_size)

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
    avg_time = sum(times)/len(times)
    update_q.put((reward_hist_list, cur_size, avg_time, reward_sum))
    avg_size = sum(sz)/len(sz)
    logger.info(f"{nb_path} AVG TIME {avg_time} AVG SCALED SIZE {avg_size}")
    # print("DONE")
    return


def manual_notebook_iter(nb_path, update_q: Queue, train_args: TrainArgs, p_id: int = 0):
    # print(nb_path)
    args = BenchArgs(expname="", nb=nb_path, sut="pod_file")
    # Load notebook.
    logger.info(f"PID {os.getpid()}, {nb_path}")
    save_file_str = (str(train_args.gamma) + "&" + str(train_args.alpha)).replace(".", "-") + str(p_id)
    nb_cells = Notebooks.nb(args=args)
    nb_exec = NotebookExecutor(nb_cells)

    bench_sizes = BENCH_DATA[nb_path]["sizes"]
    bench_times = BENCH_DATA[nb_path]["times"]

    pod_storage_path = Path(f"tmp/pod{save_file_str}")
    if pod_storage_path.exists():
        shutil.rmtree(pod_storage_path)

    hist_list = []
    reward_list = []

    # Initialize sut
    sut = StaticPodPickling(
        FilePodStorage(Path(f"tmp/pod{save_file_str}")), podding_fn=ManualPodding.podding_fn, post_podding_fn=None
    )

    last_storage_size = 0
    # expstat = ExpStat()
    pids: List[PodId] = []
    reward_sum = 0
    times = []
    sz = []
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
        times.append(dump_time)
        bench_size = bench_sizes[nth]
        bench_time = bench_times[nth]
        reward = 0.01 * ((bench_time - dump_time) / dump_time) + 100 * ((bench_size - size) / bench_size)
        sz.append((bench_size - size) / bench_size)
        times.append(dump_time)

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
    avg_time = sum(times)/len(times)
    reward_hist_list = [(reward_list[i], final_hist_list[i]) for i in range(len(final_hist_list))]
    update_q.put((reward_hist_list, cur_size, avg_time, reward_sum))
    logger.info(f"{nb_path} AVG TIME {avg_time} AVG SCALED SIZE {sum(sz)/len(sz)}")
    # print("DONE")
    return


def train(n_epochs, nbs, args: TrainArgs):
    """Trains model on n_epochs on nbs"""
    eps = 0.4
    eps_decay_factor = 0.945
    try:
        os.mkdir("qtables")
    except FileExistsError:
        pass
    with __FEATURE__:
        save_file_str = (str(args.gamma) + "&" + str(args.alpha)).replace(".", "-")
        prob_predictor = XGBPredictor(data_dir="podding_features/")
        prob_predictor.train()
        model = QLearningPoddingModel(train=True, gamma=args.gamma, alpha=args.alpha, predictor=prob_predictor)
        for n in tqdm(range(n_epochs)):
            model.set_epsilon(eps)
            logger.info(f"EPOCH = {n}, GAMMA = {args.gamma}, ALPHA = {args.alpha}, EPSILON = {eps}")
            eps *= eps_decay_factor
            procs: List[Process] = []
            update_q = Queue()
            p_id = 0
            for nb_path in nbs:
                p = Process(target=train_notebook_iter, args=(nb_path, update_q, args, model, p_id))
                procs.append(p)
                try:
                    p.start()
                    p_id += 1
                except:
                    logger.info("ERROR STARTING PROCESS")
                    return

            sizes = []
            rewards = []
            times = []
            popped = 0
            while popped < len(nbs):
                update_val, size, time, reward_sum = update_q.get()
                popped += 1
                model.batch_update_q_parallel(update_val)
                sizes.append(size)
                times.append(time)
                rewards.append(reward_sum)

            for p in procs:
                try:
                    p.join()
                except:
                    logger.info("ERROR JOINING")

            model.size_history.append(sum(sizes) / len(sizes))
            model.dump_time_history.append(sum(times)/len(times))
            avg_reward = sum(rewards) / len(rewards)
            model.reward_history.append(avg_reward)
            logger.info(f"EPOCH {n}, AVG SUM OF REWARDS {avg_reward}")
            model.save_features("podding_features/data.csv")

            if n % 10 == 0:
                model.save_q_table(f"qtables/{save_file_str}-{n}.npy")
            model.clear_action_history()

        logger.info("PLOTTING")
        model.plot_stats(name=save_file_str)
        logger.info("DONE PLOTTING")
        model.save_q_table(f"qtables/{save_file_str}.npy")
        logger.info(f"SAVED TO qtables/{save_file_str}.npy")


def eval(qt_path, nbs):
    try:
        os.mkdir("qtables")
    except FileExistsError:
        pass
    with __FEATURE__:
        model = QLearningPoddingModel(train=False, gamma=0, alpha=0, qt_path=qt_path)
        procs: List[Process] = []
        update_q = Queue()
        p_id = 0
        for nb_path in nbs:
            p = Process(target=train_notebook_iter, args=(nb_path, update_q, args, model, p_id))
            procs.append(p)
            try:
                p.start()
                p_id += 1
            except:
                logger.info("ERROR STARTING PROCESS")
                return

        sizes = []
        rewards = []
        popped = 0
        while popped < len(nbs):
            update_val, size, time, reward_sum = update_q.get()
            popped += 1
            sizes.append(size)
            rewards.append(reward_sum)

        for p in procs:
            try:
                p.join()
            except:
                logger.info("ERROR JOINING")
        print("DONE W ITERS")

        logger.info(f"AVG SIZE {sum(sizes)/len(sizes)}")
        avg_reward = sum(rewards) / len(rewards)
        logger.info(f"AVG SUM OF REWARDS {avg_reward}")
        # model.save_q_table(f"qtables/EVAL.npy")


def bench(nbs):
    with __FEATURE__:
        procs: List[Process] = []
        update_q = Queue()
        p_id = 0
        for nb_path in nbs:
            p = Process(target=manual_notebook_iter, args=(nb_path, update_q, args, p_id))
            procs.append(p)
            try:
                p.start()
                p_id += 1
            except:
                logger.info("ERROR STARTING PROCESS")
                return

        sizes = []
        rewards = []
        popped = 0
        while popped < len(nbs):
            update_val, size, reward_sum = update_q.get()
            popped += 1
            sizes.append(size)
            rewards.append(reward_sum)

        for p in procs:
            try:
                p.join()
            except:
                logger.info("ERROR JOINING")

        logger.info(f"AVG SIZE {sum(sizes)/len(sizes)}")
        avg_reward = sum(rewards) / len(rewards)
        logger.info(f"AVG SUM OF REWARDS {avg_reward}")


if __name__ == "__main__":
    # logger.info(f"Arguments {sys.argv}")
    if len(sys.argv) != 5:
        logger.error("Usage --gamma <gamma> --alpha <alpha>")
        sys.exit(2)
    args = simple_parsing.parse(TrainArgs, args=sys.argv[1:])
    train(75, [
        "notebooks/it-s-that-time-of-the-year-again.ipynb",
        "notebooks/better-xgb-baseline.ipynb",
        "notebooks/fast-fourier-transform-denoising.ipynb",
        "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
        # "notebooks/amex-dataset.ipynb",
        "notebooks/denoising-with-direct-wavelet-transform.ipynb",
        "notebooks/04_training_linear_models.ipynb"
    ], args)

    # eval("qtables/EVAL.npy", [
    #     "notebooks/it-s-that-time-of-the-year-again.ipynb",
    #     "notebooks/better-xgb-baseline.ipynb",
    #     "notebooks/fast-fourier-transform-denoising.ipynb",
    #     "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
    #     # "notebooks/amex-dataset.ipynb",
    #     "notebooks/denoising-with-direct-wavelet-transform.ipynb",
    #     "notebooks/04_training_linear_models.ipynb"
    # ])

    # bench(
    #     [
    #         "notebooks/it-s-that-time-of-the-year-again.ipynb",
    #         "notebooks/better-xgb-baseline.ipynb",
    #         "notebooks/fast-fourier-transform-denoising.ipynb",
    #         "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
    #         # "notebooks/amex-dataset.ipynb",
    #         "notebooks/denoising-with-direct-wavelet-transform.ipynb",
    #         "notebooks/04_training_linear_models.ipynb",

    #     ]
    # )

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
