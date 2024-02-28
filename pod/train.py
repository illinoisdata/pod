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

import simple_parsing
from loguru import logger
from model import QLearningPoddingModel
from tqdm import tqdm

from pod.bench import BenchArgs, NotebookExecutor, Notebooks
from pod.common import PodId
from pod.feature import __FEATURE__
from pod.pickling import ManualPodding, StaticPodPickling
from pod.storage import FilePodStorage

SIZES = [
    {
        "nb": "notebooks/simple.ipynb",
        "sizes": [110, 116, 170, 170, 219, 219, 512, 914, 973, 1075, 1198, 1254],
        "final size": 6930,
    },
    {"nb": "notebooks/numpy.ipynb", "sizes": [210, 6457, 13916, 16547, 22301, 16547], "final size": 75978},
    {
        "nb": "notebooks/denoising-with-direct-wavelet-transform.ipynb",
        "sizes": [219, 120001054, 120017161, 120017161, 120017509, 120018126, 120026251],
        "final size": 720097481,
    },
    {
        "nb": "notebooks/it-s-that-time-of-the-year-again.ipynb",
        "sizes": [
            5801,
            5903,
            90211,
            14725686,
            17164974,
            17164974,
            17678467,
            18192583,
            18706200,
            19220407,
            19905695,
            20248809,
            20934763,
            20934763,
            21277814,
            20591915,
            20779483,
            20779483,
            20817298,
            20821387,
            20825312,
            20826769,
            20831549,
            20831549,
            20844017,
            20856576,
            20856576,
            20856576,
            20856576,
            20856576,
            20858835,
            20947398,
            20947398,
            20985354,
            24253414,
            27390105,
            27390105,
            28043685,
            28697520,
            29481040,
            31571871,
            33662837,
            33662837,
            33662837,
            36799548,
            36824787,
            36824891,
            36824891,
            36826516,
            36829309,
            42057003,
            45831248,
            47530907,
            47530907,
        ],
        "final size": 1323923935,
    },
    {
        "nb": "notebooks/fast-fourier-transform-denoising.ipynb",
        "sizes": [
            493,
            799207077,
            799200675,
            799200675,
            799480115,
            799503650,
            799503650,
            799504227,
            863440323,
            863440323,
            863440732,
            869840813,
            869840813,
            869840813,
            869840813,
            869840813,
            869840813,
        ],
        "final size": 13404966818,
    },
    {
        "nb": "notebooks/04_training_linear_models.ipynb",
        "sizes": [
            967,
            2780,
            2780,
            4500,
            4500,
            4751,
            4751,
            4751,
            5106,
            5106,
            5369,
            5369,
            5582,
            5582,
            5582,
            6338,
            59352,
            59391,
            325126,
            325126,
            325718,
            325718,
            343010,
            343010,
            111892,
            111892,
            111949,
            111949,
            111949,
            113920,
            113920,
            113936,
            117260,
            118882,
            120332,
            120501,
            120706,
            120748,
            120989,
            120993,
            121026,
            121134,
            121448,
            121814,
            183633,
            184110,
            184110,
            14193794,
            16199294,
            16201052,
            16210431,
            16210431,
            16211250,
            16211821,
            16235133,
            16235256,
            16235256,
            16235256,
            22052374,
            22051610,
            24579846,
            24579846,
            24579846,
            24579846,
            24583560,
            24583560,
            24590149,
            24591815,
            24591815,
            24591815,
            24595798,
            24597482,
            24597621,
            24604976,
            24604976,
            23802437,
            23805640,
            23802760,
            23802871,
            23802871,
            31717510,
            26119151,
            26119151,
        ],
        "final size": 797387658,
    },
    {
        "nb": "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
        "sizes": [
            2951153,
            4276102,
            4589127,
            4589151,
            4901641,
            6676140,
            6676172,
            6676172,
            6905439,
            6905439,
            6754499,
            6754499,
            6756822,
            12460436,
            12460436,
            12460436,
            13175007,
            13175007,
            13175007,
            13175785,
            13175785,
            13387554,
            13387500,
            13387500,
            13387500,
            13387500,
        ],
        "final size": 245607809,
    },
    {
        "nb": "notebooks/better-xgb-baseline.ipynb",
        "sizes": [
            966,
            656040,
            23236973,
            25594677,
            25752663,
            25752663,
            25752663,
            26931610,
            26931610,
            29364867,
            29365819,
            38796843,
            38796843,
            45347237,
            47704922,
            47704922,
            47704922,
            47704922,
            47704922,
            47704922,
            47704922,
            47704922,
            47673367,
            47673367,
            47673367,
            47673398,
            47673398,
            47673398,
            47673398,
            47673394,
            47925951,
            47925951,
            47925951,
            48001068,
        ],
        "final size": 1317086858,
    },
]


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

    bench_sizes = []
    for s in SIZES:
        if s["nb"] == nb_path:
            bench_sizes = s["sizes"]
            break

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
        reward = -0.001 * dump_time + 10 * (bench_size - size) / bench_size
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
    update_q.put((reward_hist_list, cur_size, reward_sum))
    logger.info(f"{nb_path} AVG TIME {sum(times)/len(times)} AVG SCALED SIZE {sum(sz)/len(sz)}")
    # print("DONE")
    return


def bench_notebook_iter(nb_path, update_q: Queue, train_args: TrainArgs, p_id: int = 0):
    # print(nb_path)
    args = BenchArgs(expname="", nb=nb_path, sut="pod_file")
    # Load notebook.
    logger.info(f"PID {os.getpid()}, {nb_path}")
    save_file_str = (str(train_args.gamma) + "&" + str(train_args.alpha)).replace(".", "-") + str(p_id)
    nb_cells = Notebooks.nb(args=args)
    nb_exec = NotebookExecutor(nb_cells)

    bench_sizes = []
    for s in SIZES:
        if s["nb"] == nb_path:
            bench_sizes = s["sizes"]
            break

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
        reward = -0.001 * dump_time + 10 * (bench_size - size) / bench_size
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
    update_q.put((reward_hist_list, cur_size, reward_sum))
    logger.info(f"{nb_path} AVG TIME {sum(times)/len(times)} AVG SCALED SIZE {sum(sz)/len(sz)}")
    # print("DONE")
    return


def train(n_epochs, nbs, args: TrainArgs):
    """Trains model on n_epochs on nbs"""
    eps = 1.0
    eps_decay_factor = 0.987
    try:
        os.mkdir("qtables")
    except FileExistsError:
        pass
    with __FEATURE__:
        save_file_str = (str(args.gamma) + "&" + str(args.alpha)).replace(".", "-")
        model = QLearningPoddingModel(train=True, gamma=args.gamma, alpha=args.alpha)
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
            popped = 0
            while popped < len(nbs):
                update_val, size, reward_sum = update_q.get()
                popped += 1
                model.batch_update_q_parallel(update_val)
                sizes.append(size)
                rewards.append(reward_sum)

            for p in procs:
                try:
                    p.join()
                except:
                    logger.info("ERROR JOINING")

            model.size_history.append(sum(sizes) / len(sizes))
            avg_reward = sum(rewards) / len(rewards)
            model.reward_history.append(avg_reward)
            logger.info(f"EPOCH {n}, AVG SUM OF REWARDS {avg_reward}")
            if n % 10 == 0:
                model.save_q_table(f"qtables/{save_file_str}-{n}.npy")

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


def bench(nbs):
    with __FEATURE__:
        procs: List[Process] = []
        update_q = Queue()
        p_id = 0
        for nb_path in nbs:
            p = Process(target=bench_notebook_iter, args=(nb_path, update_q, args, p_id))
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
    # train(200, [
    #     "notebooks/simple.ipynb",
    #     "notebooks/it-s-that-time-of-the-year-again.ipynb",
    #     "notebooks/better-xgb-baseline.ipynb",
    #     "notebooks/fast-fourier-transform-denoising.ipynb",
    #     "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
    #     # "notebooks/amex-dataset.ipynb",
    #     "notebooks/denoising-with-direct-wavelet-transform.ipynb",
    #     "notebooks/numpy.ipynb",
    #     "notebooks/04_training_linear_models.ipynb"
    # ], args)

    # eval("qtables/0-7&0-1.npy", [
    #     "notebooks/simple.ipynb",
    #     "notebooks/it-s-that-time-of-the-year-again.ipynb",
    #     "notebooks/better-xgb-baseline.ipynb",
    #     "notebooks/fast-fourier-transform-denoising.ipynb",
    #     "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
    #     # "notebooks/amex-dataset.ipynb",
    #     "notebooks/denoising-with-direct-wavelet-transform.ipynb",
    #     "notebooks/numpy.ipynb",
    #     "notebooks/04_training_linear_models.ipynb"
    # ])

    bench(
        [
            "notebooks/simple.ipynb",
            "notebooks/it-s-that-time-of-the-year-again.ipynb",
            "notebooks/better-xgb-baseline.ipynb",
            "notebooks/fast-fourier-transform-denoising.ipynb",
            "notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb",
            # "notebooks/amex-dataset.ipynb",
            "notebooks/denoising-with-direct-wavelet-transform.ipynb",
            "notebooks/numpy.ipynb",
            "notebooks/04_training_linear_models.ipynb",
        ]
    )

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
