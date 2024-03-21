import random
from itertools import product
from pathlib import Path
from types import CodeType, FunctionType, ModuleType, NoneType
from typing import Dict, List, Optional

import matplotlib.figure
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from loguru import logger

from pod.common import Object, PodId
from pod.feature import __FEATURE__
from pod.pickling import BasePickler, PodAction, PoddingFunction
from pod.xgb_predictor import XGBPredictor


class PoddingModel:

    def podding_fn(self, obj: Object) -> PodAction:
        raise NotImplementedError("Abstract method")


class QLearningPoddingModel(PoddingModel):
    SIMPLE_TYPES = [t.__name__ for t in [
        float,
        int,
        complex,
        bool,
        tuple,
        NoneType,
        type,
    ]]

    SIMPLE_TYPE = "SIMPLE_TYPE"
    OTHER = "other"
    TYPES = [SIMPLE_TYPE] + [t.__name__ for t in [
        list,
        dict,
        str,
        bytes,
        np.ndarray,
        pd.DataFrame,
        matplotlib.figure.Figure,
        FunctionType,
        ModuleType,
        CodeType,
    ]] + [OTHER]

    SPLIT_TYPES = [t.__name__ for t in [
        # Builtin types.
        str,
        bytes,
        list,
        dict,
        # Numerical types.
        np.ndarray,
        pd.DataFrame,
        matplotlib.figure.Figure,
        # Nested types.
        FunctionType,
        CodeType,
    ]]
    FINAL_TYPES = [t.__name__ for t in [
        # Builtin types.
        str,
        bytes,
        # Numerical types.
        np.ndarray,
        pd.DataFrame,
        matplotlib.figure.Figure,
        ModuleType,
    ]]

    SIZES = [1_000_000_000, 1_000_000, 1_000, 100, 1, -2]
    PROBABILITIES = [1.0, 0.8, 0.6, 0.4, 0.2, 0.0]
    ACTION_CHOICES = list(range(len(list(PodAction)))) + [-1]
    actions = list(PodAction)
    def __init__(self, predictor: XGBPredictor, qt_path=None, train=False, alpha=0.2, gamma=0.8):
        self.state_to_qt_idx = {}
        self.action_history = {}
        idx = 0
        self.actions = list(PodAction)
        self.change_predictor = predictor
        num_rows_qt = (
            2
            * len(QLearningPoddingModel.SIZES)
            * len(QLearningPoddingModel.SIZES)
            * len(QLearningPoddingModel.PROBABILITIES)
            * len(QLearningPoddingModel.PROBABILITIES)
            * len(QLearningPoddingModel.TYPES)
            * len(QLearningPoddingModel.ACTION_CHOICES)
        )

        if qt_path is not None:
            with open(qt_path, "rb") as qtable_path:
                self.q_table = np.load(qtable_path)
        else:
            self.q_table = np.zeros((num_rows_qt, len(self.actions)))

        self.pod_action_to_action = {self.actions[i]: i for i in range(len(self.actions))}

        for idx, (has_changed, obj_s, pod_s, pod_max_change_prob, oid_change_prob, obj_type, past_action) in enumerate(
            product(
                [True, False],
                QLearningPoddingModel.SIZES,
                QLearningPoddingModel.SIZES,
                QLearningPoddingModel.PROBABILITIES,
                QLearningPoddingModel.PROBABILITIES,
                QLearningPoddingModel.TYPES,
                QLearningPoddingModel.ACTION_CHOICES,
            )
        ):
            self.state_to_qt_idx[(has_changed, obj_s, pod_s, pod_max_change_prob, oid_change_prob, obj_type, past_action)] = idx
            if qt_path is None:
                self._set_inductive_bias(obj_type, idx, has_changed, past_action)

        self.train = train
        self.features = {
            "oid": [],
            "pod_size": [],
            "pod_max_change_prob": [],
            "oid_count": [],
            "oid_change_count": [],
            "oid_change_prob": [],
            "obj_type": [],
            "obj_len": [],
            "has_changed": [],
            "past_action" : []
        }

        if self.train:
            self.epsilon = 1
            self.history = []
            self.alpha = alpha
            self.gamma = gamma
            self.reward_history = []
            self.size_history = []
            self.dump_time_history = []

    def _set_inductive_bias(self, obj_type, idx, has_changed, past_action):
        if obj_type == QLearningPoddingModel.SIMPLE_TYPE:
            self.q_table[idx, self.pod_action_to_action[PodAction.bundle]] += 10
        elif obj_type in QLearningPoddingModel.FINAL_TYPES:
            self.q_table[idx, self.pod_action_to_action[PodAction.split_final]] += 10
        elif obj_type in QLearningPoddingModel.SPLIT_TYPES:
            self.q_table[idx, self.pod_action_to_action[PodAction.split]] += 10
        if not has_changed:
            if past_action != -1:
                self.q_table[idx, past_action] += 20

    def get_features(self, obj, pickler):
        self.features["oid"].append(id(obj))
        self.features["pod_size"].append(self._get_pod_size(pickler))
        self.features["pod_max_change_prob"].append(__FEATURE__.pod_max_change_prob(pickler))
        self.features["oid_count"].append(__FEATURE__.oid_count(id(obj)))
        self.features["oid_change_count"].append(__FEATURE__.oid_change_count(id(obj)))
        self.features["oid_change_prob"].append(__FEATURE__.oid_change_prob(id(obj)))
        self.features["obj_type"].append(type(obj).__name__)
        self.features["obj_len"].append(self._get_obj_len(obj))
        self.features["past_action"].append(self.action_history.get(id(obj), -1))
        return self.features

    def _get_obj_len(self, obj: Object) -> Optional[int]:
        try:
            len_fn = getattr(obj, "__len__", None)
            if len_fn is None:
                return None
            return len_fn()
        except:
            return None

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

    def podding_fn(self, obj: Object, pickler: BasePickler, history_list: list = None) -> PodAction:
        features = self.get_features(obj, pickler)
        q_table_idx = self._get_q_table_idx(features)
        action_idx = np.argmax(self.q_table[q_table_idx])
        if self.train and random.random() < self.epsilon:
            action_idx = random.randint(0, len(self.actions) - 1)  # Both sides of range are inclusive
        if self.train:
            # self.history.append((q_table_idx, action_idx))
            if history_list:
                history_list.append((q_table_idx, action_idx))
        self.action_history[id(obj)] = action_idx
        return self.actions[action_idx]
  

    def post_podding_fn(self) -> None:
        for oid in self.features["oid"][len(self.features["has_changed"]) :]:
            self.features["has_changed"].append(__FEATURE__.has_changed(oid))
        self.save_features("features.csv")

    def set_epsilon(self, eps):
        self.epsilon = eps

    def _get_q_table_idx(self, features):
        has_changed = features["has_changed"][-1] if len(features["has_changed"]) > 0 else False
        obj_size, pod_size = self._get_size_values(features)
        pod_max_change_prob, oid_change_prob = self._get_prob_values(features)
        obj_type = features["obj_type"][-1]
        past_action = features["past_action"][-1]
        if obj_type in QLearningPoddingModel.SIMPLE_TYPES:
            obj_type = QLearningPoddingModel.SIMPLE_TYPE
        elif obj_type not in QLearningPoddingModel.TYPES:
            obj_type = QLearningPoddingModel.OTHER
        state = (has_changed, obj_size, pod_size, pod_max_change_prob, oid_change_prob, obj_type, past_action)
        return self.state_to_qt_idx[state]

    def _get_size_values(self, features):
        obj_size = -1
        pod_size = -1
        feature_obj_len = features["obj_len"][-1]
        feature_pod_size = features["pod_size"][-1]
        if feature_obj_len is None:
            feature_obj_len = -2
        if feature_pod_size is None:
            feature_pod_size = -2
        for s in self.SIZES:
            if obj_size == -1 and feature_obj_len >= s:
                obj_size = s
            if pod_size == -1 and feature_pod_size >= s:
                pod_size = s
        return obj_size, pod_size

    def _get_prob_values(self, features):
        pod_max_change_prob = -1.0
        oid_change_prob = -1.0
        for i in range(len(self.PROBABILITIES) - 2):
            hi_p, lo_p = self.PROBABILITIES[i], self.PROBABILITIES[i + 1]
            feature_pod_max_change = features["pod_max_change_prob"][-1]
            if feature_pod_max_change is None:
                feature_pod_max_change = 0.0
            if feature_pod_max_change >= lo_p and pod_max_change_prob < 0:
                if (hi_p - feature_pod_max_change) <= (feature_pod_max_change - lo_p):
                    pod_max_change_prob = hi_p
                else:
                    pod_max_change_prob = lo_p

            feature_oid_change_prob = features["oid_change_prob"][-1]
            if feature_oid_change_prob is None:
                feature_oid_change_prob = 0.0
            if feature_oid_change_prob >= lo_p and oid_change_prob < 0:
                if (hi_p - feature_oid_change_prob) <= (feature_oid_change_prob - lo_p):
                    oid_change_prob = hi_p
                else:
                    oid_change_prob = lo_p
        return max(pod_max_change_prob, 0.0), max(oid_change_prob, 0.0)
    
    def clear_action_history(self):
        self.action_history = {}

    def save_features(self, save_path: Path) -> None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.features).to_csv(save_path)

    def batch_update_q(self, reward):
        # logger.info(f"UPDATING ON {len(self.history)} ITEMS")
        self.reward_history.append(reward)
        self.history = self.history.reverse()
        final_state_idx, final_action_idx = self.history[0]
        self.q_table[final_state_idx, final_action_idx] = reward
        for i in range(1, len(self.history)):
            state_idx, action_idx = self.history[i]
            next_state_idx, _ = self.history[i - 1]
            next_max = np.max(self.q_table[next_state_idx])
            updated_q_val = (1 - self.alpha) * self.q_table[state_idx, action_idx] + self.alpha * (self.gamma * next_max)
            self.q_table[state_idx, action_idx] = updated_q_val
        self.history = []

    def batch_update_q_parallel(self, reward_hist_list):
        for reward, history in reward_hist_list:
            if len(history) > 0:
                history.reverse()
                final_state_idx, final_action_idx = history[0]
                self.q_table[final_state_idx, final_action_idx] = reward
                for i in range(1, len(history)):
                    state_idx, action_idx = history[i]
                    next_state_idx, _ = history[i - 1]
                    next_max = np.max(self.q_table[next_state_idx])
                    updated_q_val = (1 - self.alpha) * self.q_table[state_idx, action_idx] + self.alpha * (
                        self.gamma * next_max
                    )
                    self.q_table[state_idx, action_idx] = updated_q_val

    def save_q_table(self, save_path):
        with open(save_path, "wb") as qt_save_path:
            np.save(save_path, self.q_table)

    def plot_stats(self, name=""):
        plt.cla()
        plt.clf()
        plt.figure()
        plt.plot(self.reward_history)
        plt.xlabel("Epoch")
        plt.ylabel("Avg. Reward")
        plt.savefig(f"reward_plot_{name}.png")

        plt.cla()
        plt.clf()
        plt.figure()
        plt.plot(self.size_history)
        plt.xlabel("Epoch")
        plt.ylabel("Avg Storage Size")
        plt.savefig(f"size_{name}.png")
        plt.show()

        plt.cla()
        plt.clf()
        plt.figure()
        plt.plot(self.dump_time_history)
        plt.xlabel("Epoch")
        plt.ylabel("Avg dump time")
        plt.savefig(f"dump_time_{name}.png")


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
