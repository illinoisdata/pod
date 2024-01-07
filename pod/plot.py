# TODO: Test with plt

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import simple_parsing
from loguru import logger

from pod.stats import ExpStat

"""
Example:
python pod/plot.py exp1 --result_paths result/test_inmem result/test_file
"""


@dataclass
class PlotArgs:
    result_paths: List[Path]  # Path to experiment results.


def plot_exp1(argv: List[str]) -> None:
    # Parse arguments
    args = simple_parsing.parse(PlotArgs, args=argv)
    logger.info(args)

    # Read all results.
    all_results = {result_path.name: ExpStat.load(result_path) for result_path in args.result_paths}

    # Print overall stats.
    for expname, result in all_results.items():
        logger.info(f"{expname}")
        result.summary()

    # Plot exp1 (dump latency + storage + load latency).
    N, M, R, SZ = 1, 3, 1.6, 2.0
    fig, axes = plt.subplots(nrows=N, ncols=M, figsize=(SZ * R * M, SZ * N))

    # Dump latency
    ax = axes[0]
    for expname, result in all_results.items():
        latencies = [dump.time_s for dump in result.dumps]
        ax.plot(list(range(len(latencies))), latencies, label=expname)
    ax.set_xlabel("Steps")
    ax.set_ylabel("Dump Latency (s)")

    # Storage
    ax = axes[1]
    for expname, result in all_results.items():
        storages = [dump.storage_b for dump in result.dumps]
        ax.plot(list(range(len(storages))), storages, label=expname)
    ax.set_xlabel("Steps")
    ax.set_ylabel("Storage (bytes)")

    # Dump latency
    ax = axes[2]
    for expname, result in all_results.items():
        latencies = [load.time_s for load in result.loads]
        ax.plot(list(range(len(latencies))), latencies, label=expname)
    ax.legend(loc="upper left", bbox_to_anchor=(1, 1), prop={"size": 6})
    ax.set_xlabel("Steps")
    ax.set_ylabel("Load Latency (s)")

    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    logger.info(f"Arguments {sys.argv}")
    if len(sys.argv) < 2:
        logger.error("Missing experiment name, e.g., python pod/plot.py exp1")
        sys.exit(1)

    if sys.argv[1] == "exp1":
        plot_exp1(sys.argv[2:])
    else:
        logger.error(f'Unknown experiment "{sys.argv[0]}"')
        sys.exit(1)
