from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

from pathlib import Path

from pod._pod import AsyncPodObjectStorage
from pod.pickling import ManualPodding, StaticPodPickling
from pod.storage import FilePodStorage

__DEFAULT_PICKLING__ = StaticPodPickling(
    FilePodStorage(Path("/tmp/chipmink")),
    podding_fn=ManualPodding.podding_fn,
    post_podding_fn=None,
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
