from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import pickle
import random
import sys
from pathlib import Path
from types import CodeType, FunctionType, ModuleType
from typing import Dict, List, Optional

import matplotlib.figure
import numpy as np
import pandas as pd
from loguru import logger
from xgboost import XGBClassifier

from pod.common import Object, ObjectId, PodId
from pod.feature import __FEATURE__
from pod.pickling import BasePickler, PodAction, PoddingFunction
from pod.size import recursive_size

try:
    from types import NoneType
except ImportError:
    NoneType = type(None)  # type: ignore

""" Storage performance profiles """


class StorageProfile:
    def __init__(
        self,
        latency: float,  # s
        bandwidth: float,  # B/s
    ) -> None:
        self._latency = latency
        self._bandwidth = bandwidth

    def read_time(self, size: float) -> float:
        return self._latency + size / self._bandwidth


""" Change predictors """


class ChangePredictor:
    def change_prob(self, obj: Object, pickler: BasePickler) -> float:
        raise NotImplementedError("Abstract method")


class NaiveChangePredictor:
    def __init__(self, const: float) -> None:
        self._const = const

    def change_prob(self, obj: Object, pickler: BasePickler) -> float:
        return self._const


class XGBChangePredictor:
    def __init__(self, path: Path) -> None:
        self._path = path

        with open(self._path, "rb") as f:
            self._model = pickle.load(f)
            assert isinstance(self._model, XGBClassifier), self._model

    def change_prob(self, obj: Object, pickler: BasePickler) -> float:
        oid_count = __FEATURE__.oid_count(id(obj))
        oid_change_count = __FEATURE__.oid_change_count(id(obj))
        obj_len = self._get_obj_len(obj)
        return self._model.predict_proba(
            [
                [
                    oid_count,
                    oid_change_count,
                    obj_len,
                ]
            ]
        )[0, 1]

    def _get_obj_len(self, obj: Object) -> Optional[int]:
        try:
            return len(obj)
        except Exception:
            return None


class TrueChangePredictor:
    def __init__(self, path: Path) -> None:
        self._path = path

        with open(self._path, "rb") as f:
            self.hash_changed = pickle.load(f)

    def change_prob(self, obj: Object, pickler: BasePickler) -> float:
        tid = pickler.root_pid.tid
        try:
            h = hash(obj)
        except Exception:
            # print(f"NO_HASH {str(obj)[:100]} {id(obj)}")
            return 1.0
        if h not in self.hash_changed[tid]:
            # print(f"NO_DATA {str(obj)[:100]} {id(obj)}")
            return 1.0
        return 1.0 if self.hash_changed[tid][h] else 0.0


""" Cost models """


class UnifiedCostModel:
    def __init__(
        self,
        change_predictor: ChangePredictor,  # obj -> prob (unitless)
        storage_profile: StorageProfile,  # B -> s
        storage_coeff: float,  # 1/B
        write_coeff: float,  # 1/s
        read_coeff: float,  # 1/s
        pod_speed: float,  # B/s
        pickle_speed: float,  # B/s
    ) -> None:
        self._change_predictor = change_predictor
        self._storage_profile = storage_profile
        self._storage_coeff = storage_coeff
        self._write_coeff = write_coeff
        self._read_coeff = read_coeff
        self._pod_speed = pod_speed
        self._pickle_speed = pickle_speed

    def bundle_cost(self, obj: Object, pickler: BasePickler) -> float:
        change_prob = self._change_predictor.change_prob(obj, pickler)
        bundle_size = self._get_pod_size(pickler) + sys.getsizeof(obj)
        return self.cost(change_prob=change_prob, size=bundle_size, use_pod=True)

    def remaining_final_cost(self, obj: Object) -> float:
        remaining_size = recursive_size(obj)
        return self.cost(change_prob=1.0, size=remaining_size, use_pod=False)

    def cost(self, change_prob: float, size: float, use_pod: bool) -> float:
        storage_cost = change_prob * size
        write_cost = size / (self._pod_speed if use_pod else self._pickle_speed)
        read_cost = self._storage_profile.read_time(size)
        weighted_storage_cost = self._storage_coeff * storage_cost
        weighted_write_cost = self._write_coeff * write_cost
        weighted_read_cost = self._read_coeff * read_cost
        # print(f"p= {change_prob}, s= {size} -> ls= {storage_cost}, lw= {write_cost}, lr= {read_cost}")
        # print(
        #     f"p= {change_prob}, s= {size} -> "
        #     f"ls= {weighted_storage_cost}, lw= {weighted_write_cost}, lr= {weighted_read_cost}"
        # )
        return weighted_storage_cost + weighted_write_cost + weighted_read_cost

    def _get_pod_size(self, pickler: BasePickler) -> Optional[int]:
        file = getattr(pickler, "file", None)
        if file is None:
            logger.warning("Unexpectedly missing pickler's field: file.")
            return None
        return len(file.getvalue())


""" Podding models """


class PoddingModel:
    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        raise NotImplementedError("Abstract method")

    def post_podding_fn(self) -> None:
        pass


class RandomPoddingModel:
    def __init__(self, weights: Optional[List[float]] = None) -> None:
        self.weights = weights if weights is not None else [1.0 / 2, 1.0 / 4, 1.0 / 4]
        self.actions = list(PodAction)
        assert len(self.weights) == len(self.actions)

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        return random.choices(self.actions, weights=self.weights, k=1)[0]


class PreservingPoddingModel(PoddingModel):
    def __init__(self) -> None:
        self._prev_actions: Dict[ObjectId, PodAction] = {}
        self._curr_actions: Dict[ObjectId, PodAction] = {}

        assert __FEATURE__.in_block and "track_change" in __FEATURE__.cfg

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        if self._pod_has_changed(obj):
            action = self.podding_fn_impl(obj, pickler)  # Plan a new action for this object.
        else:
            # TODO: If last split without split, we can turn them to split-final.
            action = self._prev_actions[id(obj)]  # Repeat the same action from last save.
        self._curr_actions[id(obj)] = action
        return action

    def post_podding_fn(self) -> None:
        self._prev_actions = self._curr_actions
        self._curr_actions = {}

    def _pod_has_changed(self, obj: Object) -> bool:
        if id(obj) not in self._prev_actions:
            return True  # Unseen object always changes.

        # First time always changes, so we need to filter it.
        return __FEATURE__.oid_change_count(id(obj)) > 1 and __FEATURE__.has_changed(id(obj))

    def podding_fn_impl(self, obj: Object, pickler: BasePickler) -> PodAction:
        raise NotImplementedError("Abstract method")


class ConservativePoddingModel(PoddingModel):
    def __init__(self) -> None:
        self._actions: Dict[ObjectId, PodAction] = {}

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        if id(obj) not in self._actions:
            self._actions[id(obj)] = self.podding_fn_impl(obj, pickler)
            # if self._actions[id(obj)] == PodAction.split:
            #     print(f"new conservative, id= {id(obj)}, action= {self._actions[id(obj)]}")
        return self._actions[id(obj)]

    def podding_fn_impl(self, obj: Object, pickler: BasePickler) -> PodAction:
        raise NotImplementedError("Abstract method")


# class PackPoddingModel(PreservingPoddingModel):
    # def __init__(self, cost_model: UnifiedCostModel, max_pod_size: float) -> None:
    #     PreservingPoddingModel.__init__(self)

class PackPoddingModel(ConservativePoddingModel):
    def __init__(self, cost_model: UnifiedCostModel, max_pod_size: float) -> None:
        ConservativePoddingModel.__init__(self)

        self._cost_model = cost_model
        self._max_pod_size = max_pod_size
        self._max_cost_per_pod = self._cost_model.cost(
            change_prob=1.0,
            size=self._max_pod_size,
            use_pod=True,
        )

    def podding_fn_impl(self, obj: Object, pickler: BasePickler) -> PodAction:
        # if not self._pod_has_changed(pickler.root_obj):
        #     return PodAction.split
        # if self._cost_model._get_pod_size(pickler) >= self._max_pod_size:
        #     return PodAction.split

        """ Pack by unified cost """

        # # Bundle this object if it fits in the current pod.
        # bundle_cost = self._cost_model.bundle_cost(obj, pickler)
        # if bundle_cost <= self._max_cost_per_pod:
        #     return PodAction.bundle
        # # Split this object, but finalize if remaining cost is low enough.
        # # print(
        # #     f"Split at obj size= {sys.getsizeof(obj)}, pod size= {self._cost_model._get_pod_size(pickler)} "
        # #     f"({bundle_cost} > {self._max_cost_per_pod})"
        # # )
        # remaining_cost = self._cost_model.remaining_final_cost(obj)
        # if remaining_cost < self._max_cost_per_pod:
        #     return PodAction.split_final
        # return PodAction.split

        # """ Pack by size """

        # bundle_cost = self._cost_model._get_pod_size(pickler) + sys.getsizeof(obj)
        # if bundle_cost <= self._max_pod_size:
        #     return PodAction.bundle
        # # remaining_cost = recursive_size(obj)
        # # if remaining_cost <= self._max_pod_size:
        # #     return PodAction.split_final
        # return PodAction.split

        """ Pack size by change rate """

        # root_oid_count = __FEATURE__.oid_count(id(pickler.root_obj))
        # root_oid_change_count = __FEATURE__.oid_change_count(id(pickler.root_obj))
        # root_oid_change_prob = 0 if root_oid_count == 0 else root_oid_change_count / root_oid_count
        # oid_count = __FEATURE__.oid_count(id(obj))
        # oid_change_count = __FEATURE__.oid_change_count(id(obj))
        # oid_change_prob = 0 if oid_count == 0 else oid_change_count / oid_count
        # if root_oid_change_count != oid_change_count:
        #     print(
        #         f"MISMATCH {str(obj)[:200]} {oid_change_count} / {oid_count} split from {str(pickler.root_obj)[:100]}, {root_oid_change_count} / {root_oid_count}"
        #     )
        #     return PodAction.split
        # # if oid_change_prob > root_oid_change_prob:
        # #     print(
        # #         f"BAD_PROB {str(obj)[:200]} {oid_change_count} / {oid_count} split from {str(pickler.root_obj)[:100]}, {root_oid_change_count} / {root_oid_count}"
        # #     )
        # #     return PodAction.split
        # bundle_cost = self._cost_model._get_pod_size(pickler) + sys.getsizeof(obj)
        # max_pod_size = max(1e6, self._max_pod_size / 2 ** max(oid_change_count - 1, 0))
        # # print(max_pod_size, oid_change_count)
        # if bundle_cost <= max_pod_size:
        #     return PodAction.bundle
        # # print(
        # #     f"OVERPACK {str(obj)[:200]} {oid_change_count} / {oid_count} split from {str(pickler.root_obj)[:100]}, {root_oid_change_count} / {root_oid_count}"
        # # )
        # return PodAction.split

        """ Pack by change probability """

        change_prob = self._cost_model._change_predictor.change_prob(obj, pickler)
        if change_prob < 1.0:
            return PodAction.bundle
        # remaining_cost = self._cost_model.remaining_final_cost(obj)
        # if remaining_cost < self._max_cost_per_pod:
        #     return PodAction.split_final
        return PodAction.split


class GreedyPoddingModel(ConservativePoddingModel):
    IMMUTABLE_TYPES = (
        str,
        bytes,
        int,
        float,
        complex,
        bool,
        tuple,
        NoneType,
        type,
    )

    def __init__(self) -> None:
        ConservativePoddingModel.__init__(self)

        self._pod_cr: Dict[PodId, float] = {}
        self._pod_overhead: float = 120.0  # bytes per save.

    def podding_fn_impl(self, obj: Object, pickler: BasePickler) -> PodAction:
        if pickler.root_pid not in self._pod_cr:
            self._pod_cr[pickler.root_pid] = self._change_rate(pickler.root_obj)
        pod_cr = self._pod_cr[pickler.root_pid]
        obj_cr = self._change_rate(obj)
        pod_size = self._get_pod_size(pickler)
        obj_size = sys.getsizeof(obj)
        bundle_cost = (pod_size + obj_size) * (pod_cr + obj_cr) + self._pod_overhead
        split_cost = pod_size * pod_cr + obj_size * obj_cr + 2 * self._pod_overhead
        split_final_cost = float("inf")  # Not consider for now
        min_cost = min(bundle_cost, split_cost, split_final_cost)
        # print(f"{bundle_cost=}, {split_cost=}, {split_final_cost=}")
        if bundle_cost == min_cost:
            self._pod_cr[pickler.root_pid] = pod_cr + obj_cr
            return PodAction.bundle
        elif split_cost == min_cost:
            return PodAction.split
        elif split_final_cost == min_cost:
            return PodAction.split_final
        raise ValueError(f"Unreachable min({bundle_cost}, {split_cost}, {split_final_cost}) = {min_cost}")

    def post_podding_fn(self) -> None:
        _post_podding_fn = getattr(super(), "post_podding_fn", None)
        if _post_podding_fn is not None:
            _post_podding_fn()
        self._pod_cr = {}

    def _change_rate(self, obj: Object) -> float:
        # TODO: Model better.
        if isinstance(obj, GreedyPoddingModel.IMMUTABLE_TYPES):
            return 0.0
        return 0.5

    def _get_pod_size(self, pickler: BasePickler) -> Optional[int]:
        file = getattr(pickler, "file", None)
        if file is None:
            logger.warning("Unexpectedly missing pickler's field: file.")
            return None
        return len(file.getvalue())


class ManualPreservePoddingModel(PreservingPoddingModel):
    BUNDLE_TYPES = (
        # Constant/small size.
        float,
        int,
        complex,
        bool,
        tuple,
        NoneType,
    )
    SPLIT_TYPES = (
        # Builtin types.
        str,
        bytes,
        list,
        dict,
        type,
        # Numerical types.
        np.ndarray,
        pd.DataFrame,
        matplotlib.figure.Figure,
        # Nested types.
        FunctionType,
        ModuleType,
        CodeType,
    )
    FINAL_TYPES = (
        # Builtin types.
        str,
        bytes,
        type,
        # Numerical types.
        np.ndarray,
        pd.DataFrame,
        matplotlib.figure.Figure,
    )
    SPLIT_MODULES = {
        "matplotlib",
        "sklearn",
    }
    FINAL_MODULES = {
        "matplotlib",
        "sklearn",
    }
    CHECK_SIZE_TYPES = (
        str,
        bytes,
        list,
        dict,
        np.ndarray,
        pd.DataFrame,
    )

    def __init__(self) -> None:
        PreservingPoddingModel.__init__(self)

    def podding_fn_impl(self, obj: Object, pickler: BasePickler) -> PodAction:
        if isinstance(obj, ManualPreservePoddingModel.BUNDLE_TYPES):
            return PodAction.bundle

        # Always bundle small objects.
        # if isinstance(obj, ManualPreservePoddingModel.CHECK_SIZE_TYPES):
        if sys.getsizeof(obj) < 1e3:
            return PodAction.bundle

        # Extract object module.
        obj_module = getattr(obj, "__module__", None)
        obj_module = obj_module.split(".")[0] if isinstance(obj_module, str) else None
        is_split = (
            isinstance(obj, ManualPreservePoddingModel.SPLIT_TYPES) or obj_module in ManualPreservePoddingModel.SPLIT_MODULES
        )
        is_split_final = is_split and (
            isinstance(obj, ManualPreservePoddingModel.FINAL_TYPES) or obj_module in ManualPreservePoddingModel.FINAL_MODULES
        )

        # Decide whether to split.
        if is_split:
            # root_oid_count = __FEATURE__.oid_count(id(pickler.root_obj))
            # root_oid_change_count = __FEATURE__.oid_change_count(id(pickler.root_obj))
            # oid_count = __FEATURE__.oid_count(id(obj))
            # oid_change_count = __FEATURE__.oid_change_count(id(obj))
            # if is_split_final:
            #     # print(
            #     #     f"FINAL {str(obj)[:100]} {oid_change_count} / {oid_count} split from {str(pickler.root_obj)[:100]}, {root_oid_change_count} / {root_oid_count}"
            #     # )
            #     return PodAction.split_final
            # print(
            #     f"SPLIT {str(obj)[:100]} {oid_change_count} / {oid_count} split from {str(pickler.root_obj)[:100]}, {root_oid_change_count} / {root_oid_count}"
            # )
            return PodAction.split
        else:
            return PodAction.bundle

    def _get_obj_len(self, obj: Object) -> Optional[int]:
        len_fn = getattr(obj, "__len__", None)
        if len_fn is None:
            return 0
        return len_fn()


class FeatureCollectorModel:
    def __init__(self, save_path: Path, podding_fn: PoddingFunction) -> None:
        self._save_path = save_path
        self._podding_fn = podding_fn

        self.features: Dict[str, list] = {
            "oid": [],
            "pod_size": [],
            "pod_max_change_prob": [],
            "oid_count": [],
            "oid_change_count": [],
            "oid_change_prob": [],
            "obj_type": [],
            "obj_len": [],
            "has_changed": [],
        }

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        self.features["oid"].append(id(obj))
        self.features["pod_size"].append(self._get_pod_size(pickler))
        self.features["pod_max_change_prob"].append(__FEATURE__.pod_max_change_prob(pickler))
        self.features["oid_count"].append(__FEATURE__.oid_count(id(obj)))
        self.features["oid_change_count"].append(__FEATURE__.oid_change_count(id(obj)))
        self.features["oid_change_prob"].append(__FEATURE__.oid_change_prob(id(obj)))
        self.features["obj_type"].append(type(obj).__name__)
        self.features["obj_len"].append(self._get_obj_len(obj))
        return self._podding_fn(obj, pickler)

    def post_podding_fn(self) -> None:
        for oid in self.features["oid"][len(self.features["has_changed"]) :]:
            self.features["has_changed"].append(__FEATURE__.has_changed(oid))
        self.save(save_path=self._save_path)

    def save(self, save_path: Path) -> None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.features).to_csv(save_path)

    def _get_pod_size(self, pickler: BasePickler) -> Optional[int]:
        file = getattr(pickler, "file", None)
        if file is None:
            logger.warning("Unexpectedly missing pickler's field: file.")
            return None
        return len(file.getvalue())

    def _get_pod_id(self, pickler: BasePickler) -> Optional[PodId]:
        root_pid = getattr(pickler, "root_pid", None)
        if root_pid is None:
            logger.warning("Unexpectedly missing pickler's field: root_pid.")
            return None
        return root_pid

    def _get_pod_max_change_prob(self, pickler: BasePickler) -> Optional[float]:
        root_pid = self._get_pod_id(pickler)
        if root_pid is None:
            return None
        return __FEATURE__.pod_max_change_prob(root_pid)

    def _get_obj_len(self, obj: Object) -> Optional[int]:
        len_fn = getattr(obj, "__len__", None)
        if len_fn is None:
            return None
        return len_fn()


class ChangeCollectorModel:
    def __init__(self, save_path: Path) -> None:
        self._save_path = save_path

        self.hash_changed: Dict[TimeId, Dict[int, bool]] = {}
        self.recent_objs: List[Object] = []

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        self.recent_objs.append(obj)
        return PodAction.split

    def post_podding_fn(self) -> None:
        tid = len(self.hash_changed)
        self.hash_changed[tid] = {}
        for obj in self.recent_objs:
            try:
                self.hash_changed[tid][hash(obj)] = __FEATURE__.has_changed(id(obj))
            except Exception:
                pass
        self.recent_objs = []
        self.save(save_path=self._save_path)

    def save(self, save_path: Path) -> None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            pickle.dump(self.hash_changed, f)
