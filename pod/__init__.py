from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

from pathlib import Path

from pod._pod import AsyncPodObjectStorage
from pod.model import GreedyPoddingModel, LightGBMClassifierRoC
from pod.pickling import StaticPodPickling
from pod.storage import FilePodStorage

__DEFAULT_MODEL__ = GreedyPoddingModel(
    roc_model=LightGBMClassifierRoC.load_from(Path("models/roc_lgb.txt")),
    pod_overhead=1200,
)
__DEFAULT_PICKLING__ = StaticPodPickling(
    FilePodStorage(Path("/tmp/chipmink")),
    podding_fn=__DEFAULT_MODEL__.podding_fn,
    post_podding_fn=__DEFAULT_MODEL__.post_podding_fn,
)
__DEFAULT_POD__ = AsyncPodObjectStorage(
    __DEFAULT_PICKLING__,
    active_filter=True,
    always_lock_all=False,
)

new_managed_namespace = __DEFAULT_POD__.new_managed_namespace
save = __DEFAULT_POD__.save
load = __DEFAULT_POD__.load

__all__ = [
    # Get new namespace (dictionary) managed by Chipmink.
    "new_managed_namespace",
    # Store current namespace state in Chipmink.
    "save",
    # Retrieve all or subset of namespace at a specific time.
    "load",
]
