from pod.bench import Notebooks, NotebookExecutor, BenchArgs
from pod.pickling import StaticPodPickling
from pod.storage import DictPodStorage
from model import QLearningPoddingModel
from pod.stats import ExpStat
from pod.feature import __FEATURE__
from typing import List
import time
from pod.common import PodId
from loguru import logger
import gc
import random
import numpy as np

def train(n_epochs, nbs):
    """Trains model on n_epochs on nbs"""
    eps = 1.0
    eps_decay_factor = 0.9
    with __FEATURE__:
        model = QLearningPoddingModel(train=True)
        for n in range(n_epochs):
            print(f"EPOCH {n}")
            model.set_epsilon(eps)
            eps *= eps_decay_factor
            for nb_path in nbs:
                args = BenchArgs(expname="", nb=nb_path, sut="inmem_dict")
                # Load notebook.
                nb_cells = Notebooks.nb(args=args)
                nb_exec = NotebookExecutor(nb_cells)

                # Initialize sut
                sut = StaticPodPickling(DictPodStorage(), podding_fn=model.podding_fn, post_podding_fn=model.feature_collector.post_podding_fn(save=False))

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

                    # Reset environment to reduce noise.
                    gc.collect()

                    # Rudimentary reward function based on dump time/storage size, this can be changed to include load time as well
                    size = sut.estimate_size()
                    dump_time = dump_stop_ts - dump_start_ts
                    reward = -1000*dump_time + -1*size
                    print(f"SIZE {size}")
                    print(f"TIME {dump_time}")
                    print(f"REWARD {reward}")

                    # Updates Q values based on actions made for dumps
                    model.batch_update_q(reward=reward)

                logger.info(f"Collected pids {pids}")
                
                # Load random steps.
                for nth, idx in enumerate(random.choices(range(len(pids)), k=10)):
                    # Load state.
                    load_start_ts = time.time()
                    try:
                        the_locals = sut.load(pids[idx])
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
                print("UNIQUE VALS")
                print(np.unique(model.q_table, return_counts=True))
        model.plot_rewards()

if __name__ == "__main__":
    train(5, ["notebooks/simple.ipynb", "rmlist"])