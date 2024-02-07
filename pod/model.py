import random
from typing import List, Optional

from pod.common import Object
from pod.pickling import BasePickler, PodAction


class PoddingModel:
    def podding_fn(self, obj: Object) -> PodAction:
        raise NotImplementedError("Abstract method")


class RandomPoddingModel:
    def __init__(self, weights: Optional[List[float]] = None) -> None:
        self.weights = weights if weights is not None else [1.0 / 2, 1.0 / 4, 1.0 / 4]
        self.actions = [PodAction.bundle, PodAction.split, PodAction.split_final]

    def podding_fn(self, obj: Object, pickler: BasePickler) -> PodAction:
        return random.choices(self.actions, weights=self.weights, k=1)[0]
