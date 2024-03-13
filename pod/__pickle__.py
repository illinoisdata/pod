from __future__ import annotations  # isort:skip

import os
import pickle
from dataclasses import dataclass
from typing import Any, Dict, List, Set

# Use Python pickle for testing pod prototype. When ideas are solidified and pod is implemented in C, revert this back.
pickle.Pickler = pickle._Pickler  # type: ignore
pickle.Unpickler = pickle._Unpickler  # type: ignore
pickle.dump = pickle._dump  # type: ignore
pickle.dumps = pickle._dumps  # type: ignore
pickle.load = pickle._load  # type: ignore
pickle.loads = pickle._loads  # type: ignore


# Specify base pickle module at import time. Supported base pickles: dill, cloudpickle, pickle.
BASE_PICKLE = os.environ.get("POD_BASE_PICKLE", "dill").lower()


if BASE_PICKLE == "dill":
    # Must be imported after the pickle switch.
    import dill  # noqa: E402

    # Fix infinite recursion on dill.detect.globalvars
    # For example, def f(x): return g() if x else 0, def g(): return f(False)
    # Real example, https://github.com/scikit-learn/scikit-learn/blob/main/sklearn/base.py clone and _clone_parametrized

    @dataclass
    class TarjanPage:
        names: Set[str]  # Set of variable names.
        globs: Dict[str, Any]  # Namespace.
        index: int  # Index of the current Tarjan page.
        low_index: int  # Lowest descendent index of the current Tarjan page.
        on_stack: bool
        has_seen: bool

    class PodDetect:
        def __init__(self) -> None:
            self._book: Dict[int, TarjanPage] = {}
            self._index_counter = 0
            self._page_stack: List[Any] = []

        def _see(self, page_1: TarjanPage, page_2: TarjanPage) -> None:
            page_1.names.update(page_2.names)
            if not page_2.has_seen:
                page_2.has_seen = True
                page_1.low_index = min(page_1.low_index, page_2.low_index)
            elif page_2.on_stack:
                page_1.low_index = min(page_1.low_index, page_2.index)

        # Copied and modified from dill.detect
        def globalvars(self, func: Any, recurse: bool = True, builtin: bool = False) -> Dict[str, Any]:
            """get objects defined in global scope that are referred to by func

            return a dict of {name:object}"""

            if id(func) in self._book:
                this_page = self._book[id(func)]
                return dict((name, this_page.globs[name]) for name in this_page.names if name in this_page.globs)

            this_page = TarjanPage(
                names=set(), globs={}, index=self._index_counter, low_index=self._index_counter, on_stack=True, has_seen=False
            )
            self._book[id(func)] = this_page
            self._index_counter += 1
            self._page_stack.append(func)

            if dill.detect.ismethod(func):
                this_page.names = func.__func__
            if dill.detect.isfunction(func):
                this_page.globs = vars(dill.detect.getmodule(sum)).copy() if builtin else {}
                # get references from within closure
                orig_func, this_page.names = func, set()
                for obj in orig_func.__closure__ or {}:
                    try:
                        cell_contents = obj.cell_contents
                    except ValueError:  # cell is empty
                        pass
                    else:
                        _vars = self.globalvars(cell_contents, recurse, builtin) or {}
                        self._see(this_page, self._book[id(cell_contents)])
                        this_page.globs.update(_vars)
                # get globals
                this_page.globs.update(orig_func.__globals__ or {})
                # get names of references
                if not recurse:
                    this_page.names.update(orig_func.__code__.co_names)
                else:
                    this_page.names.update(dill.detect.nestedglobals(orig_func.__code__))
                    # find globals for all entries of func
                    for key in this_page.names.copy():  # XXX: unnecessary...?
                        nested_func = this_page.globs.get(key)
                        if nested_func is orig_func:
                            # func.remove(key) if key in func else None
                            continue  # XXX: globalvars(func, False)?
                        _ = self.globalvars(nested_func, True, builtin)
                        self._see(this_page, self._book[id(nested_func)])
            elif dill.detect.iscode(func):
                this_page.globs = vars(dill.detect.getmodule(sum)).copy() if builtin else {}
                # globs.update(globals())
                if not recurse:
                    this_page.names = func.co_names  # get names
                else:
                    orig_func = func.co_name  # to stop infinite recursion
                    this_page.names = set(dill.detect.nestedglobals(func))
                    # find globals for all entries of func
                    for key in this_page.names.copy():  # XXX: unnecessary...?
                        if key is orig_func:
                            # func.remove(key) if key in func else None
                            continue  # XXX: globalvars(func, False)?
                        nested_func = this_page.globs.get(key)
                        _ = self.globalvars(nested_func, True, builtin)
                        self._see(this_page, self._book[id(nested_func)])
            # NOTE: if name not in __globals__, then we skip it...
            if this_page.index == this_page.low_index:
                while True:
                    next_func = self._page_stack.pop()
                    next_page = self._book[id(next_func)]
                    next_page.on_stack = False
                    next_page.has_seen = True
                    this_page.names.update(next_page.names)
                    next_page.names = this_page.names
                    if next_func is func:
                        break
                    # print(f"SCC: {func} <--> {next_func}")
            return dict((name, this_page.globs[name]) for name in this_page.names if name in this_page.globs)

        @staticmethod
        def safe_globalvars(func: Any, recurse: bool = True, builtin: bool = False) -> Dict[str, Any]:
            return PodDetect().globalvars(func, recurse, builtin)

    # pod_detect = PodDetect()
    # dill.detect.globalvars = pod_detect.globalvars
    dill.detect.globalvars = PodDetect.safe_globalvars

elif BASE_PICKLE == "cloudpickle":
    # Must be imported after the pickle switch.
    import cloudpickle  # noqa: E402

    # Revert the dispatch table patching because it collides with pure-Python pickle.
    # When pod is fully integrated with C pickle and the pickle switch is reverted, this should be safe to remove.
    cloudpickle.Pickler.dispatch = pickle.Pickler.dispatch.copy()

elif BASE_PICKLE == "pickle":
    pass

else:
    raise ValueError(f"Invalid BASE_PICKLE= {BASE_PICKLE}")
