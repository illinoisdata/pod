from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

# import ast...

from pod._pod import Namespace


class StaticCodeChecker:
    def is_static(self, code: str, namespace: Namespace) -> bool:
        raise NotImplementedError("Abstract method")


class AlwaysNonStaticCodeChecker(StaticCodeChecker):
    def __init__(self):
        pass

    def is_static(self, code: str, namespace: Namespace) -> bool:
        return False


class AllowlistStaticCodeChecker(StaticCodeChecker):
    def __init__(self):
        pass

    def is_static(self, code: str, namespace: Namespace) -> bool:
        # TODO(Sumay): To be implemented...
        return False
