from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import inspect
import pickle
import random
import sys
from copyreg import dispatch_table
from pathlib import Path
from typing import Dict, List, Optional

import lightgbm as lgb
import numpy as np
import pandas as pd
from loguru import logger
from xgboost import XGBRegressor

from pod.common import Object, ObjectId, PodDependencyMap, PodId
from pod.feature import __FEATURE__
from pod.pickling import BasePickler, PodAction, PoddingFunction

try:
    from types import NoneType
except ImportError:
    NoneType = type(None)  # type: ignore


""" Rate of Change Models """


class RateOfChangeModel:
    IMMUTABLE_TYPES = (
        # Primitive immutable types.
        str,
        bytes,
        int,
        float,
        complex,
        bool,
        # Immutable collections.
        tuple,
        frozenset,
        # Meta and singleton types.
        NoneType,
        type,
    )

    def roc(self, obj: Object, pickler: BasePickler) -> float:
        raise NotImplementedError("Abstract method")


class XGBRegressorRoC(RateOfChangeModel):
    def __init__(self, bst: XGBRegressor) -> None:
        self._bst = bst

    @staticmethod
    def load_from(path: Path) -> XGBRegressorRoC:
        with open(path, "rb") as f:
            bst = pickle.load(f)
        return XGBRegressorRoC(bst)

    def roc(self, obj: Object, pickler: BasePickler) -> float:
        if isinstance(obj, RateOfChangeModel.IMMUTABLE_TYPES):
            return 0.0

        # Extract features. Should match those in RoCFeatureCollectorModel and training script.
        features = [
            sys.getsizeof(obj),  # size
            self._get_obj_len(obj),  # len
            self._get_obj_len_dict(obj),  # len_dict
        ]

        # Run through XGBRegressor.
        return min(max(self._bst.predict([features])[0], 0.0), 1.0)

    def _get_obj_len(self, obj: Object) -> Optional[int]:
        try:
            return obj.__len__()
        except Exception:
            return None

    def _get_obj_len_dict(self, obj: Object) -> Optional[int]:
        if hasattr(obj, "__dict__"):
            return len(obj.__dict__)
        return None


class LightGBMClassifierRoC(RateOfChangeModel):
    def __init__(self, bst: lgb.Booster) -> None:
        self._bst = bst
        self._features = np.array([[0.0, 0.0, 0.0]])

    @staticmethod
    def load_from(path: Path) -> LightGBMClassifierRoC:
        return LightGBMClassifierRoC(lgb.Booster(model_file=path.resolve()))

    def roc(self, obj: Object, pickler: BasePickler) -> float:
        if isinstance(obj, RateOfChangeModel.IMMUTABLE_TYPES):
            return 0.0

        # Extract features. Should match those in RoCFeatureCollectorModel and training script.
        self._features[0, 0] = sys.getsizeof(obj)  # size
        self._features[0, 1] = self._get_obj_len(obj)  # len
        self._features[0, 2] = self._get_obj_len_dict(obj)  # len_dict

        # Run through XGBRegressor.
        if random.random() < 0.1 and self._bst.predict(self._features, num_threads=1)[0] < 0.5:
            return 0.0
        return 0.1

    def _get_obj_len(self, obj: Object) -> Optional[int]:
        if not hasattr(obj, "__len__"):
            return None
        try:
            return obj.__len__()
        except Exception:
            return None

    def _get_obj_len_dict(self, obj: Object) -> Optional[int]:
        if hasattr(obj, "__dict__"):
            return len(obj.__dict__)
        return None


class ConstRoCModel(RateOfChangeModel):
    def __init__(self, roc: float) -> None:
        self._roc = roc

    def roc(self, obj: Object, pickler: BasePickler) -> float:
        return self._roc


""" PoddingModel """


class PoddingModel:
    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        raise NotImplementedError("Abstract method")


class NaivePoddingModel(PoddingModel):
    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        return PodAction.split


class NaiveBundlePoddingModel(PoddingModel):
    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        return PodAction.bundle


class FixedDecisionPoddingModel(PoddingModel):
    def __init__(self, decision_string: int, num_decisions: int, enable_log: bool = False) -> None:
        self._decision_string = decision_string  # Integer represents a string of bits.
        self._num_decisions = num_decisions
        self._enable_log = enable_log
        self._decision_count = 0

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        if isinstance(obj, (PodId,)) or (isinstance(obj, str) and obj in ["pod.common", "PodId"]) or obj == PodId:
            return PodAction.bundle  # Preset to reduce decision count (always better than split).
        next_decision = self._decision_string % 2
        self._decision_string = self._decision_string // 2
        if self._enable_log:
            self._decision_count += 1
            if self._decision_count > self._num_decisions:
                logger.warning(f"Ran out of decision, decision_count= {self._decision_count}")
        return PodAction.bundle if next_decision == 0 else PodAction.split


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


class GreedyPoddingModel(ConservativePoddingModel):
    SPLIT_FINAL_AT_DEP = 1

    def __init__(
        self,
        roc_model: RateOfChangeModel,
        pod_overhead: float,  # bytes per save.
    ) -> None:
        ConservativePoddingModel.__init__(self)
        self._roc_model = roc_model
        self._pod_overhead = pod_overhead

        self._pod_cr: Dict[PodId, float] = {}

    def podding_fn_impl(self, obj: Object, pickler: BasePickler) -> PodAction:
        if pickler.root_pid not in self._pod_cr:
            self._pod_cr[pickler.root_pid] = self._roc_model.roc(pickler.root_obj, pickler)
        pod_cr = self._pod_cr[pickler.root_pid]
        obj_cr = self._roc_model.roc(obj, pickler)
        bundle_cr = min(pod_cr + obj_cr, 1.0)
        pod_size = self._get_pod_size(pickler)
        obj_size = sys.getsizeof(obj)
        bundle_cost = (pod_size + obj_size) * bundle_cr + self._pod_overhead
        split_cost = pod_size * pod_cr + obj_size * obj_cr + 2 * self._pod_overhead
        split_final_cost = float("inf")  # Not considered for now.
        min_cost = min(bundle_cost, split_cost, split_final_cost)
        # print(f"{bundle_cost=}, {split_cost=}, {split_final_cost=}")
        if bundle_cost == min_cost:
            self._pod_cr[pickler.root_pid] = bundle_cr
            return PodAction.bundle
        elif split_cost == min_cost:
            if pickler.pod_depth >= GreedyPoddingModel.SPLIT_FINAL_AT_DEP:
                return PodAction.split_final
            return PodAction.split
        elif split_final_cost == min_cost:
            return PodAction.split_final
        raise ValueError(f"Unreachable min({bundle_cost}, {split_cost}, {split_final_cost}) = {min_cost}")

    def post_podding_fn(self, dependency_map: PodDependencyMap) -> None:
        _post_podding_fn = getattr(super(), "post_podding_fn", None)
        if _post_podding_fn is not None:
            _post_podding_fn()
        self._pod_cr = {}

    def _get_pod_size(self, pickler: BasePickler) -> int:
        file = getattr(pickler, "file", None)
        if file is None:
            logger.warning("Unexpectedly missing pickler's field: file.")
            return 0
        return len(file.getvalue())


class RandomPoddingModel:
    def __init__(self, weights: Optional[List[float]] = None) -> None:
        self.weights = weights if weights is not None else [1.0 / 2, 1.0 / 4, 1.0 / 4]
        self.actions = list(PodAction)
        assert len(self.weights) == len(self.actions)

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        return random.choices(self.actions, weights=self.weights, k=1)[0]


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

    def post_podding_fn(self, dependency_map: PodDependencyMap) -> None:
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


class RoCFeatureCollectorModel:
    NAMESPACE: Dict[str, Object] = {}

    def __init__(self, feature_path: Path, change_path: Path, dep_path: Path, var_path: Path) -> None:
        self._feature_path = feature_path
        self._change_path = change_path
        self._dep_path = dep_path
        self._var_path = var_path
        self.dump_th = 0
        self.oid_counters: Dict[int, int] = {}
        self.features: Dict[str, list] = {
            "oid": [],
            "global": [],
            "type": [],
            "type_global": [],
            "type_pickler_dispatch": [],
            "type_subclass_type": [],
            "type_module": [],
            "type_module_global": [],
            "has_reduce_ex": [],
            "has_reduce": [],
            "size": [],
            "len": [],
            "len_dict": [],
            "len_slots": [],
        }
        self.changes: Dict[str, list] = {
            "oid": [],
            "root_oid": [],
            "nth": [],
            "dump_th": [],
            "has_changed": [],
        }
        self.deps: Dict[str, list] = {
            "nth": [],
            "dump_th": [],
            "from_root_oid": [],
            "to_root_oid": [],
        }
        self.vars: Dict[str, list] = {
            "dump_th": [],
            "varname": [],
            "oid": [],
        }

        assert __FEATURE__.is_enabled and "track_change" in __FEATURE__.cfg

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        if id(obj) not in self.oid_counters:
            self.oid_counters[id(obj)] = 0

            type_module = inspect.getmodule(type(obj))
            namespace_objs = RoCFeatureCollectorModel.NAMESPACE.values()  # TODO: Skip activating.

            def is_global(obj):
                return any(namespace_obj is obj for namespace_obj in namespace_objs)

            self.features["oid"].append(id(obj))
            self.features["global"].append(is_global(obj))
            self.features["type"].append(type(obj).__name__)
            self.features["type_global"].append(is_global(type(obj)))
            self.features["type_pickler_dispatch"].append(type(obj) in self._get_dispatch_table(pickler))
            self.features["type_subclass_type"].append(issubclass(type(obj), type))
            self.features["type_module"].append(None if type_module is None else type_module.__name__)
            self.features["type_module_global"].append(is_global(type_module))
            self.features["has_reduce"].append(hasattr(obj, "__reduce__") and callable(obj.__reduce__))
            self.features["has_reduce_ex"].append(hasattr(obj, "__reduce_ex__") and callable(obj.__reduce_ex__))
            self.features["size"].append(sys.getsizeof(obj))
            self.features["len"].append(self._get_obj_len(obj))
            self.features["len_dict"].append(self._get_obj_len_dict(obj))
            self.features["len_slots"].append(self._get_obj_len_slots(obj))
        return PodAction.split

    def post_podding_fn(self, dependency_map: PodDependencyMap) -> None:
        pids: Dict[PodId, int] = {}
        for varname, oid in __FEATURE__.variables().items():
            self.vars["dump_th"].append(self.dump_th)
            self.vars["varname"].append(varname)
            self.vars["oid"].append(oid)
        for oid in self.oid_counters:
            pid = __FEATURE__.pid_for_oid(oid)
            pids[pid] = self.oid_counters[oid]
            self.changes["oid"].append(oid)
            self.changes["root_oid"].append(pid.oid)
            self.changes["nth"].append(self.oid_counters[oid])
            self.changes["dump_th"].append(self.dump_th)
            self.changes["has_changed"].append(__FEATURE__.has_changed(oid))
            self.oid_counters[oid] += 1
        for from_pid, nth in pids.items():
            if from_pid not in dependency_map:
                continue
            for to_pid in dependency_map[from_pid].dep_pids:
                self.deps["nth"].append(nth)
                self.deps["dump_th"].append(self.dump_th)
                self.deps["from_root_oid"].append(from_pid.oid)
                self.deps["to_root_oid"].append(to_pid.oid)
        self.dump_th += 1
        self.save()

    def save(self) -> None:
        self._feature_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.features).to_csv(self._feature_path)
        self._change_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.changes).to_csv(self._change_path)
        self._dep_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.deps).to_csv(self._dep_path)
        self._var_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.vars).to_csv(self._var_path)

    def _get_dispatch_table(self, pickler: BasePickler) -> dict:
        return getattr(pickler, "dispatch_table", dispatch_table)

    def _get_obj_len(self, obj: Object) -> Optional[int]:
        try:
            return obj.__len__()
        except Exception:
            return None

    def _get_obj_len_dict(self, obj: Object) -> Optional[int]:
        if hasattr(obj, "__dict__"):
            return len(obj.__dict__)
        return None

    def _get_obj_len_slots(self, obj: Object) -> Optional[int]:
        if hasattr(obj, "__slots__"):
            return len(obj.__slots__)
        return None
