from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Set, Union

from dataclasses_json import dataclass_json
from loguru import logger

from pod.common import Object, PodId


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


def strf_storage(storage_b: Union[int, float]) -> str:
    if storage_b >= 1e9:
        return f"{storage_b / 1e9:.1f} GB"
    if storage_b >= 1e6:
        return f"{storage_b / 1e6:.1f} MB"
    if storage_b >= 1e3:
        return f"{storage_b / 1e3:.1f} KB"
    return f"{storage_b:.1f}  B"


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
        logger.info(
            f"{len(self.dumps)} dumps"
            f", avgt= {strf_deltatime(dump_avg_t_s)} ({strf_throughput(1.0/dump_avg_t_s)}), "
            f"s= {strf_storage(self.dumps[-1].storage_b)}"
        )
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


class FillDepStatus(Enum):
    empty = 0
    filling = 1
    filled = 2


@dataclass
class PodStat:
    root_type: str
    root_size: int

    fill_dep: FillDepStatus
    num_deps: int
    num_recursive_deps: int
    recursive_size: int


@dataclass
class PodTypeStat:
    nth: int
    root_type: str
    root_size: int
    num_deps: int
    num_recursive_deps: int
    recursive_size: int


_self_dependency_seen: bool = False


@dataclass
class PodPicklingStat:
    pod_stats: Dict[PodId, PodStat] = field(default_factory=lambda: {})

    def append(self, root_pid: PodId, root_obj: Object, root_bytes: bytes) -> None:
        self.pod_stats[root_pid] = PodStat(
            root_type=type(root_obj).__qualname__,
            root_size=len(root_bytes),
            fill_dep=FillDepStatus.empty,
            num_deps=-1,
            num_recursive_deps=-1,
            recursive_size=-1,
        )

    def fill_dep(self, root_pid: PodId, dependency_maps: Dict[PodId, Set[PodId]]) -> None:
        global _self_dependency_seen
        pod_stat = self.pod_stats[root_pid]
        if pod_stat.fill_dep == FillDepStatus.filling:
            if not _self_dependency_seen:
                logger.warning("Self dependency detected while fill_dep")
                _self_dependency_seen = True
            return
        if pod_stat.fill_dep == FillDepStatus.filled:
            return
        pod_stat.fill_dep = FillDepStatus.filling
        pod_stat.num_deps = len(dependency_maps[root_pid])
        pod_stat.num_recursive_deps = len(dependency_maps[root_pid])
        pod_stat.recursive_size = pod_stat.root_size
        for dep_pid in dependency_maps[root_pid]:
            self.fill_dep(dep_pid, dependency_maps)
            dep_stat = self.pod_stats[dep_pid]
            pod_stat.num_recursive_deps += dep_stat.num_recursive_deps
            pod_stat.recursive_size += dep_stat.recursive_size
        pod_stat.fill_dep = FillDepStatus.filled

    def summary(self) -> None:
        stats_by_type: Dict[str, PodTypeStat] = {}
        for pid, pod_stat in self.pod_stats.items():
            # print(f"{pod_stat.root_type}, s= {pod_stat.root_size}, tots= {pod_stat.recursive_size}")
            if pod_stat.root_type not in stats_by_type:
                stats_by_type[pod_stat.root_type] = PodTypeStat(
                    nth=0,
                    root_type=pod_stat.root_type,
                    root_size=0,
                    num_deps=0,
                    num_recursive_deps=0,
                    recursive_size=0,
                )
            stats_by_type[pod_stat.root_type].nth += 1
            stats_by_type[pod_stat.root_type].root_size += pod_stat.root_size
            stats_by_type[pod_stat.root_type].num_deps += pod_stat.num_deps
            stats_by_type[pod_stat.root_type].num_recursive_deps += pod_stat.num_recursive_deps
            stats_by_type[pod_stat.root_type].recursive_size += pod_stat.recursive_size
        for root_type, type_stat in stats_by_type.items():
            print(
                f"type= {root_type:>10s}, nth= {type_stat.nth:4d}, "
                f"avg_s= {strf_storage(type_stat.root_size / type_stat.nth):>8s}, "
                f"avg_tots= {strf_storage(type_stat.recursive_size / type_stat.nth):>8s}, "
                f"avg_deps= {type_stat.num_deps / type_stat.nth:6.1f}, "
                f"avg_totds= {type_stat.num_recursive_deps / type_stat.nth:6.1f}"
            )