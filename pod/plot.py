# TODO: Test with plt

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import simple_parsing
from loguru import logger

from pod.stats import ExpStat

"""
Example:

python pod/plot.py exp1 --result_paths result/test_inmem result/test_file

python pod/plot.py exp1batch --batch_args \
    rmlist:result/exp1_snp_itsttime,result/exp1_imm_rmlist,result/exp1_pfl_rmlist \
    storesfg:result/exp1_snp_storesfg
"""


def set_fontsize(
    ax: matplotlib.axes.Axes,
    fontsize: float,
    minor_fontsize: float,
) -> None:
    for item in [
        ax.title,
        ax.xaxis.label,
        ax.yaxis.label,
        ax.xaxis.get_offset_text(),
        ax.yaxis.get_offset_text(),
        *ax.get_xticklabels(),
        *ax.get_yticklabels(),
    ]:
        item.set_fontsize(fontsize)
    for item in [
        ax.xaxis.get_offset_text(),
        ax.yaxis.get_offset_text(),
        *ax.get_xticklabels(),
        *ax.get_yticklabels(),
    ]:
        item.set_fontsize(minor_fontsize)


@dataclass
class PlotExp1Args:
    result_paths: List[Path]  # Path to experiment results.


def plot_exp1(argv: List[str]) -> None:
    # Parse arguments
    args = simple_parsing.parse(PlotExp1Args, args=argv)
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

    # Dump latency
    ax = axes[1]
    for expname, result in all_results.items():
        latencies = [load.time_s for load in result.loads]
        ax.plot(list(range(len(latencies))), latencies, label=expname)
    ax.legend(loc="upper left", bbox_to_anchor=(1, 1), prop={"size": 6})
    ax.set_xlabel("Steps")
    ax.set_ylabel("Load Latency (s)")

    # Storage
    ax = axes[2]
    for expname, result in all_results.items():
        storages = [dump.storage_b for dump in result.dumps]
        ax.plot(list(range(len(storages))), storages, label=expname)
    ax.set_xlabel("Steps")
    ax.set_ylabel("Storage (bytes)")

    fig.tight_layout()
    plt.show()


@dataclass
class PlotExp1BatchSingleArgs:
    name: str  # Setting name.
    result_paths: List[Path]  # Path to experiment results.


@dataclass
class PlotExp1BatchArgs:
    singles: List[PlotExp1BatchSingleArgs]  # Arges for different settings.

    def __str__(self) -> str:
        return "PlotExp1BatchArgs:\n" + "\n".join("\t" + str(single) for single in self.singles)


@dataclass
class PlotExp1BatchPreArgs:
    # TODO: Find more suitable arguments
    batch_args: List[str]  # Example: nameA:path1,path2, nameB:path3,path4

    def post_parse(self) -> PlotExp1BatchArgs:
        singles: List[PlotExp1BatchSingleArgs] = []
        for single_args in self.batch_args:
            try:
                name, str_result_paths = single_args.split(":")
                result_paths = [Path(str_result_path) for str_result_path in str_result_paths.split(",")]
                singles.append(PlotExp1BatchSingleArgs(name=name, result_paths=result_paths))
            except ValueError:
                logger.error("Expect arguments in format like rmlist:result/test_pfl,result/test_snp)")
                sys.exit(1)
        return PlotExp1BatchArgs(singles=singles)


def plot_exp1batch(argv: List[str]) -> None:
    # Parse arguments
    args = simple_parsing.parse(PlotExp1BatchPreArgs, args=argv).post_parse()
    logger.info(args)

    # Plot exp1 (dump latency + storage + load latency).
    N, M, R, SZ, FS, SFS = 5, len(args.singles), 2.5, 0.8, 7, 5
    fig, axes = plt.subplots(nrows=N, ncols=M, figsize=(SZ * R * M, SZ * N))

    for single, axs in zip(args.singles, axes.T):
        assert len(axs) == N
        logger.info("============================")
        logger.info(f"{single.name}")

        # Read all results.
        all_results = [
            (f"{idx}_{result_path.parent.name}", ExpStat.load(result_path))
            for idx, result_path in enumerate(single.result_paths)
        ]

        # Print summary..
        for expname, result in all_results:
            logger.info(f"{expname}")
            result.summary()

        # Aggregate (dump time, load tim, dump storage increments) measurements.
        labels: List[str] = []
        dump_times: List[List[float]] = []
        load_times: List[List[float]] = []
        dump_final_storage_gb: List[float] = []
        dump_storage_inc_gb: List[List[float]] = []
        total_times: Dict[str, List[float]] = {
            "exec": [],
            "lock": [],
            "wait": [],
            "save": [],
        }
        for expname, result in all_results:
            labels.append(expname)
            dump_times.append([stat.time_s for stat in result.dumps])
            load_times.append([stat.time_s for stat in result.loads])
            dump_final_storage_gb.append(result.dumps[-1].storage_b / 1e9)
            dump_storage_gb = [stat.storage_b / 1e9 for stat in result.dumps]
            dump_storage_inc_gb.append(
                [
                    dump_storage_gb[idx] if idx == 0 else dump_storage_gb[idx] - dump_storage_gb[idx - 1]
                    for idx in range(len(dump_storage_gb))
                ]
            )

            # Update total times.
            total_exec_time = sum(result.exec_times)
            total_lock_time = sum(result.lock_times)
            total_join_time = sum(result.join_times)
            total_dump_time = sum(stat.time_s for stat in result.dumps)
            total_times["exec"].append(total_exec_time - total_lock_time)
            total_times["lock"].append(total_lock_time)
            total_times["wait"].append(total_join_time)
            total_times["save"].append(total_dump_time - total_join_time)

        # Plot total time breakdown.
        ax = axs[0]
        time_names = [
            "exec",
            "lock",
            "wait",
            "save",
        ]
        bottoms = np.zeros_like(labels, dtype="float64")
        for time_name in time_names:
            times = np.array(total_times[time_name])
            ax.barh(labels, times, label=time_name, left=bottoms)
            bottoms += times
        ax.set_xlabel("Time Breakdown (s)")
        ax.minorticks_on()
        ax.yaxis.set_tick_params(which="minor", bottom=False)
        ax.set_xlim(left=0)
        ax.legend(
            ncol=len(time_names),
            bbox_to_anchor=(0.5, 1.05),
            loc="lower center",
            prop={"size": 3},
        )

        ax.set_title(f"{single.name}", y=1.2)

        # Plot dump time.
        ax = axs[1]
        ax.boxplot(dump_times, labels=labels, vert=False)
        ax.set_xlabel("Dump Latency (s)")
        ax.minorticks_on()
        ax.yaxis.set_tick_params(which="minor", bottom=False)
        ax.set_xlim(left=0)

        # Plot load time.
        ax = axs[2]
        ax.boxplot(load_times, labels=labels, vert=False)
        ax.set_xlabel("Load Latency (s)")
        ax.minorticks_on()
        ax.yaxis.set_tick_params(which="minor", bottom=False)
        ax.set_xlim(left=0)

        # Plot dump stroage increments.
        ax = axs[3]
        ax.barh(list(range(len(dump_final_storage_gb))), dump_final_storage_gb)
        ax.set_xlabel("Storage (GB)")
        ax.set_yticks(list(range(len(labels))))
        ax.set_yticklabels(labels)
        ax.minorticks_on()
        ax.yaxis.set_tick_params(which="minor", bottom=False)
        ax.set_xlim(left=0)

        # Plot dump stroage increments.
        ax = axs[4]
        ax.boxplot(dump_storage_inc_gb, labels=labels, vert=False)
        ax.set_xlabel("Storage inc. (GB)")
        ax.minorticks_on()
        ax.yaxis.set_tick_params(which="minor", bottom=False)
        ax.set_xlim(left=0)

    for ax in axes.flatten():
        set_fontsize(ax, FS, SFS)

    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    logger.info(f"Arguments {sys.argv}")
    if len(sys.argv) < 2:
        logger.error("Missing experiment name, e.g., python pod/plot.py exp1")
        sys.exit(1)

    if sys.argv[1] == "exp1":
        plot_exp1(sys.argv[2:])
    elif sys.argv[1] == "exp1batch":
        plot_exp1batch(sys.argv[2:])
    else:
        logger.error(f'Unknown experiment "{sys.argv[0]}"')
        sys.exit(1)
