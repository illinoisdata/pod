import random
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.figure
import numpy as np
import pandas as pd
from loguru import logger

from pod.common import Object, PodId
from pod.feature import __FEATURE__
from pod.pickling import BasePickler, PodAction, PoddingFunction

from types import CodeType, FunctionType, ModuleType, NoneType


class PoddingModel:

    def podding_fn(self, obj: Object) -> PodAction:
        raise NotImplementedError("Abstract method")


class QLearningPoddingModel(PoddingModel):
    SIMPLE_TYPES = [
        float,
        int,
        complex,
        bool,
        tuple,
        NoneType,
        type,
        str
    ]
    SIMPLE_TYPE = "SIMPLE_TYPE"
    OTHER = "other"
    TYPES = [ 
        SIMPLE_TYPE,
        list, 
        dict,
        np.ndarray, 
        pd.DataFrame, 
        matplotlib.figure.Figure,
        FunctionType,
        ModuleType,
        CodeType,
        OTHER
    ]
    SIZES = [1_000_000_000, 1_000_000, 1_000, 100, 1, -2]
    PROBABILITIES = [1.0, 0.8, 0.6, 0.4, 0.2, 0.0, None]

    def __init__(self, train=False):
        self.state_to_qt_idx = {}
        idx = 0
        self.actions = list(PodAction)
        for has_changed in [True, False]: # has changed
            for obj_s in QLearningPoddingModel.SIZES: # current obj len
                for pod_s in QLearningPoddingModel.SIZES:
                    for pod_max_change_prob in QLearningPoddingModel.PROBABILITIES: 
                        for oid_change_prob in QLearningPoddingModel.PROBABILITIES:
                            for obj_type in QLearningPoddingModel.TYPES:
                                self.state_to_qt_idx[(has_changed, obj_s, pod_s, pod_max_change_prob, oid_change_prob, obj_type)] = idx
                                idx += 1

        self.q_table = np.zeros((len(self.state_to_qt_idx), len(self.actions)))
        self.feature_collector = FeatureCollectorModel("", None)
        self.action_to_pod_action = {i: self.actions[i] for i in range(len(self.actions))}
        self.pod_action_to_action = {self.actions[i]: i for i in range(len(self.actions))}
        self.train = train
        if self.train:
            self.epsilon = 0
            self.history = []
            self.alpha = 0.15
            self.gamma = 0.6
            self.reward_history = []
    
    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        features = self.feature_collector.get_features(obj, pickler)
        q_table_idx = self._get_q_table_idx(features)
        action = np.argmax(self.q_table[q_table_idx])
        if self.train and random.uniform(0, 1) < self.epsilon:
            action = random.choice([i for i in range(len(self.actions))])
        if self.train:
            self.history.append((q_table_idx, action))
        return self.action_to_pod_action[action]

    def set_epsilon(self, eps):
        self.epsilon = eps

    def _get_q_table_idx(self, features):
        has_changed = features["has_changed"][-1] if len(features["has_changed"]) > 0 else False
        obj_size, pod_size = self._get_size_values(features)
        pod_max_change_prob, oid_change_prob = self._get_prob_values(features)
        obj_type = features["obj_type"][-1]
        if obj_type in QLearningPoddingModel.SIMPLE_TYPES:
            obj_type = QLearningPoddingModel.SIMPLE_TYPE
        if obj_type not in QLearningPoddingModel.TYPES:
            obj_type = QLearningPoddingModel.OTHER
        state = (has_changed, obj_size, pod_size, pod_max_change_prob, oid_change_prob, obj_type)
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
            hi_p, lo_p = self.PROBABILITIES[i], self.PROBABILITIES[i+1]
            feature_pod_max_change = features["pod_max_change_prob"][-1]
            if feature_pod_max_change is None:
                feature_pod_max_change = 0.0
            if lo_p is None:
                lo_p = 0.0
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
        return pod_max_change_prob, oid_change_prob
    
    def batch_update_q(self, reward):
        print(f"UPDATING ON {len(self.history)} ITEMS")
        self.reward_history.append(reward)
        final_state_idx, final_action_idx = self.history[0]
        self.q_table[final_state_idx, final_action_idx] = reward
        for i in range(1, len(self.history)):
            state_idx, action_idx = self.history[i]
            next_state_idx, _ = self.history[i-1]
            next_max = np.max(self.q_table[next_state_idx])
            updated_q_val = (1 - self.alpha) * self.q_table[state_idx, action_idx] + self.alpha * (self.gamma * next_max)
            self.q_table[state_idx, action_idx] = updated_q_val
        self.history = []
    
    def plot_rewards(self):
        # Can't plt.show when running on docker apparently, so printing them out to plot on other machine
        print(self.reward_history)

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
    
    def _reset_features(self):
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
        }

    def get_features(self, obj, pickler):
        self.features["oid"].append(id(obj))
        self.features["pod_size"].append(self._get_pod_size(pickler))
        self.features["pod_max_change_prob"].append(__FEATURE__.pod_max_change_prob(pickler))
        self.features["oid_count"].append(__FEATURE__.oid_count(id(obj)))
        self.features["oid_change_count"].append(__FEATURE__.oid_change_count(id(obj)))
        self.features["oid_change_prob"].append(__FEATURE__.oid_change_prob(id(obj)))
        self.features["obj_type"].append(type(obj).__name__)
        self.features["obj_len"].append(self._get_obj_len(obj))
        feature_dict = dict(self.features)
        # self._reset_features()
        return feature_dict


    def post_podding_fn(self, save=True) -> None:
        for oid in self.features["oid"][len(self.features["has_changed"]) :]:
            self.features["has_changed"].append(__FEATURE__.has_changed(oid))
        if save:
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
