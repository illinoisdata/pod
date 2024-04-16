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
from pod.plot_macros import *

"""
Example:

python pod/plot.py exp1 --result_paths result/test_inmem result/test_file

python pod/plot.py exp1batch --batch_args \
    rmlist:result/exp1_snp_itsttime,result/exp1_imm_rmlist,result/exp1_pfl_rmlist \
    storesfg:result/exp1_snp_storesfg
"""
END_BAR_TEX = r"""
\label{fig:pod-storage}
\vspace{-2mm}
\end{figure*}"""

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
    plt.savefig("test_plot.png")
    plt.show()



def create_table(labels, data, name):
    table_head = "  ".join(labels)
    table_vals = create_table_vals(data)
    table_str = r"""\pgfplotstableread{"""
    table_str += table_head + "\n"
    table_str += (
        table_vals
        + r"""
}\TBL"""
        + f"{name}\n"
    )
    return table_str



def gen_storage_bar(argv: List[str]):
    args = simple_parsing.parse(PlotExp1BatchPreArgs, args=argv).post_parse()
    logger.info(args)
    tex = get_header(len(args.singles), 0.35)
    tables = []
    axes = []
    labels: List[List] = []
    suts = set()
    for single in args.singles:
        exp_labels: List[str] = []
        dump_final_storage_gb: List[float] = []
        logger.info("============================")
        logger.info(f"{single.name}")
        all_results = [
            (f"{idx}_{result_path.parent.name}", ExpStat.load(result_path))
            for idx, result_path in enumerate(single.result_paths)
        ]
        exp_labels.append("idx")
        for expname, result in all_results:
            exp_labels.append(expname)
            suts.add(expname.split("_")[2])
            dump_final_storage_gb.append(result.dumps[-1].storage_b / 1e9)
        tables.append(create_table(exp_labels, dump_final_storage_gb, single.name))
        axes.append(create_bar_axis(exp_labels, dump_final_storage_gb))
        labels.append(exp_labels)

    tex += "\n\n".join(tables) + "\n\n"
    tex += BAR_STYLE + "\n\n"
    tex += get_legend(list(suts))
    for i, single in enumerate(args.singles):
        plots = create_bar_plots(labels[i], single.name)
        tex += SUBFIG_HEADER
        tex += axes[i]
        tex += plots
        # tex += r"""\caption{ """+ single.name + r" }" + "\n"
        tex +=  r"""\end{axis}
\end{tikzpicture}
\caption{""" + single.name + r"""}
\end{subfigure}
"""
        if i < len(args.singles) - 1:
            tex += r"""\hspace{80pt}""" + "\n"

    tex += END_BAR_TEX
    # print(tex)
    with open("storagebar.tex", "w") as tf:
        tf.write(tex)

def gen_time_bar(argv: List[str]):
    """
    all {category} time in one table
    """
    args = simple_parsing.parse(PlotExp1BatchPreArgs, args=argv).post_parse()
    lock_times = []
    save_times = []
    axes = []
    labels: List[List] = []
    tex = get_header(len(args.singles), 0.5)
    for single in args.singles:
        exp_labels: List[str] = []
        exp_lock_times = []
        exp_save_times = []
        logger.info(f"{single.name}")
        all_results = [
            (f"{idx}_{result_path.parent.name}", ExpStat.load(result_path))
            for idx, result_path in enumerate(single.result_paths)
        ]

        for expname, result in all_results:
            exp_labels.append(expname)
            total_lock_time = sum(result.lock_times)
            total_join_time = sum(result.join_times)
            total_dump_time = sum(stat.time_s for stat in result.dumps)
            # exp_exec_times.append(total_exec_time - total_lock_time)
            exp_lock_times.append(total_lock_time)
            # exp_wait_times.append(total_join_time)
            exp_save_times.append((total_dump_time - total_join_time) + total_join_time)
        labels.append(exp_labels)

        # exec_times.append(exp_exec_times)
        lock_times.append(exp_lock_times)
        # wait_times.append(exp_wait_times)
        save_times.append(exp_save_times)

        # exec_table = create_time_table(exp_exec_times, single.name + "Exec")
        lock_table = create_time_table(exp_lock_times, single.name + "Lock")
        # wait_table = create_time_table(exp_wait_times, single.name + "Wait")
        save_table = create_time_table(exp_save_times, single.name + "Save")

        tex += lock_table + "\n\n"
        tex += save_table + "\n\n"

        axes.append(create_stacked_bar_axis(exp_lock_times, exp_save_times, exp_labels))

    tex += STACKED_BAR_STYLE
    tex += get_legend(["lock", "save"])
    for i, single in enumerate(args.singles):
        tex += SUBFIG_HEADER
        tex += axes[i]
        tex += create_stacked_bar_plots(single.name)
        tex +=  r"""\end{axis}
\end{tikzpicture}
\caption{""" + single.name + r"""}
\end{subfigure}
"""
    tex += END_BAR_TEX
    # print(tex)
    with open("timebd.tex", "w") as tbf:
        tbf.write(tex)


def gen_load_time_line(argv: List[str]):
    args = simple_parsing.parse(PlotExp1BatchPreArgs, args=argv).post_parse()
    logger.info(args)

    fig = get_header(3, 0)
    axes = []
    labels: List[List] = []
    plots = []
    
    suts = set()
    for single in args.singles:
        logger.info("============================")
        logger.info(f"{single.name}")

        # Read all results.
        all_results = [
            (f"{idx}_{result_path.parent.name}", ExpStat.load(result_path))
            for idx, result_path in enumerate(single.result_paths)
        ]

        sut_load_time = {}
        labels: List[str] = []

        # load_times: List[List[float]] = []

        for expname, result in all_results:
            logger.info(f"{expname}")
            labels.append(expname)
            sut = expname.split("_")[2]
            suts.add(sut)
            if sut not in sut_load_time:
                sut_load_time[sut] = {}
            sut_time = sut_load_time[sut]
            for s in result.loads:
                if s.tid not in sut_time:
                    sut_time[s.tid] = []
                sut_time[s.tid].append(s.time_s)
        # logger.info(sut_load_time)
        sut_avg_time = {}
        for sut, stats in sut_load_time.items():
            sut_avg_time[sut] = []
            for tid, times in stats.items():
                avg_time = sum(times)/len(times)
                sut_avg_time[sut].append((tid, avg_time))
            sut_avg_time[sut].sort()
        tables, names = create_line_table(sut_avg_time, single.name)
        fig += tables + "\n"
        cur_plots = create_line_plots(names)
        axis = create_line_axis(sut_avg_time)
        axes.append(axis)
        plots.append(cur_plots)
    
    fig += get_legend(list(suts))
    for i, single in enumerate(args.singles):
        fig += SUBFIG_HEADER + "\n"
        fig += axes[i] + "\n"
        fig += plots[i] + "\n"
        fig +=  r"""\end{axis}
\end{tikzpicture}
\caption{""" + single.name + r"""}
\end{subfigure}
"""
        if i < len(args.singles) - 1:
            fig += r"""\hspace{150pt}""" + "\n"
    
    fig += END_BAR_TEX
    with open("load_time.tex", "w") as lf:
        lf.write(fig)

                

"""export output=log_yj_new.txt; echo "@@@@@@@@@@@@" >> $output; (docker compose -f experiments/docker-compose.yml -p pod_sumayst2 exec podnogil bash experiments/bench_exp1.sh pna ai4code) 2>& 1 | tee -a $output; unset output;

python pod/plot.py texfigs --batch_args ai4code:result/exp1_pfa_ai4code/expstat.json,result/exp1_pga_ai4code/expstat.json,result/exp1_snp_ai4code/expstat.json covid193:result/exp1_pfa_covid193/expstat.json,result/exp1_pga_covid193/expstat.json,result/exp1_snp_covid193/expstat.json denoisdw:result/exp1_pfa_denoisdw/expstat.json,result/exp1_pga_denoisdw/expstat.json,result/exp1_snp_denoisdw/expstat.json,result/exp1_pnv_denoisdw/expstat.json skltweet:result/exp1_pfa_skltweet/expstat.json,result/exp1_pga_skltweet/expstat.json,result/exp1_snp_skltweet/expstat.json,result/exp1_pnv_skltweet/expstat.json twittnet:result/exp1_pfa_twittnet/expstat.json,result/exp1_pga_twittnet/expstat.json,result/exp1_snp_twittnet/expstat.json,result/exp1_pnv_twittnet/expstat.json
"""



    






        




if __name__ == "__main__":
    logger.info(f"Arguments {sys.argv}")
    if len(sys.argv) < 2:
        logger.error("Missing experiment name, e.g., python pod/plot.py exp1")
        sys.exit(1)

    if sys.argv[1] == "exp1":
        plot_exp1(sys.argv[2:])
    elif sys.argv[1] == "texfigs":
        gen_load_time_line(sys.argv[2:])
        gen_storage_bar(sys.argv[2:])
        gen_time_bar(sys.argv[2:])
    elif sys.argv[1] == "exp1batch":
        plot_exp1batch(sys.argv[2:])
    elif sys.argv[1] == "texbar":
        gen_storage_bar(sys.argv[2:])
    elif sys.argv[1] == "timebar":
        gen_time_bar(sys.argv[2:])
    else:
        logger.error(f'Unknown experiment "{sys.argv[0]}"')
        sys.exit(1)
