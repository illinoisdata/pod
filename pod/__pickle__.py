from __future__ import annotations  # isort:skip

import os
import pickle as _pickle
from dataclasses import dataclass
from typing import Any, Dict, List, Set

# Use Python pickle for testing pod prototype. When ideas are solidified and pod is implemented in C, revert this back.
_pickle.Pickler = _pickle._Pickler  # type: ignore
_pickle.Unpickler = _pickle._Unpickler  # type: ignore
_pickle.dump = _pickle._dump  # type: ignore
_pickle.dumps = _pickle._dumps  # type: ignore
_pickle.load = _pickle._load  # type: ignore
_pickle.loads = _pickle._loads  # type: ignore


if _pickle._HAVE_PICKLE_BUFFER:  # type: ignore

    def _save_picklebuffer(self, obj):
        if self.proto < 5:
            raise _pickle.PicklingError("PickleBuffer can only pickled with " "protocol >= 5")
        with obj.raw() as m:
            if not m.contiguous:
                raise _pickle.PicklingError("PickleBuffer can not be pickled when " "pointing to a non-contiguous buffer")
            in_band = True
            if self._buffer_callback is not None:
                in_band = bool(self._buffer_callback(obj))
            if in_band:
                # Write data in-band
                # XXX The C implementation avoids a copy here
                if m.readonly:
                    self.save(m.tobytes())  # PATCH: Fail memoize if empty bytes.
                else:
                    self.save(m.tobytes())  # PATCH: Fail memoize if empty bytes.
            else:
                # Write data out-of-band
                self.write(_pickle.NEXT_BUFFER)
                if m.readonly:
                    self.write(_pickle.READONLY_BUFFER)

    _pickle.Pickler.dispatch[_pickle.PickleBuffer] = _save_picklebuffer


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
            this_func = func
            self._book[id(this_func)] = this_page
            self._index_counter += 1
            self._page_stack.append(this_func)

            if dill.detect.ismethod(func):
                func = func.__func__

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
                    if next_func is this_func:
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
    import cloudpickle.cloudpickle as _cloudpickle  # noqa: E402

    # Revert the dispatch table patching because it collides with pure-Python pickle.
    # When pod is fully integrated with C pickle and the pickle switch is reverted, this should be safe to remove.
    _cloudpickle.Pickler.dispatch = _pickle.Pickler.dispatch.copy()

    # def _save_reduce_pickle5(
    #     self,
    #     func,
    #     args,
    #     state=None,
    #     listitems=None,
    #     dictitems=None,
    #     state_setter=None,
    #     obj=None,
    # ):
    #     save = self.save
    #     write = self.write
    #     self.save_reduce(
    #         func,
    #         args,
    #         state=None,
    #         listitems=listitems,
    #         dictitems=dictitems,
    #         obj=obj,
    #     )
    #     # backport of the Python 3.8 state_setter pickle operations
    #     save(state_setter)
    #     save(obj)  # simple BINGET opcode as obj is already memoized.
    #     save(state)
    #     write(_pickle.TUPLE2)
    #     # Trigger a state_setter(obj, state) function call.
    #     write(_pickle.REDUCE)
    #     # The purpose of state_setter is to carry-out an
    #     # inplace modification of obj. We do not care about what the
    #     # method might return, so its output is eventually removed from
    #     # the stack.
    #     write(_pickle.POP)

    # def save_global(self, obj, name=None, pack=_cloudpickle.struct.pack):
    #     """Main dispatch method.

    #     The name of this method is somewhat misleading: all types get
    #     dispatched here.
    #     """
    #     if obj is type(None):  # noqa
    #         return self.save_reduce(type, (None,), obj=obj)
    #     elif obj is type(Ellipsis):
    #         return self.save_reduce(type, (Ellipsis,), obj=obj)
    #     elif obj is type(NotImplemented):
    #         return self.save_reduce(type, (NotImplemented,), obj=obj)
    #     elif obj in _cloudpickle._BUILTIN_TYPE_NAMES:
    #         return self.save_reduce(
    #             _builtin_type, (_cloudpickle._BUILTIN_TYPE_NAMES[obj],), obj=obj
    #         )

    #     if name is not None:
    #         _pickle.Pickler.save_global(self, obj, name=name)
    #     elif not _cloudpickle._should_pickle_by_reference(obj, name=name):
    #         _save_reduce_pickle5(self, *_dynamic_class_reduce(obj), obj=obj)
    #     else:
    #         _pickle.Pickler.save_global(self, obj, name=name)

    # _cloudpickle.Pickler.dispatch[type] = save_global

    # def save_function(self, obj, name=None):
    #     """Registered with the dispatch to handle all function types.

    #     Determines what kind of function obj is (e.g. lambda, defined at
    #     interactive prompt, etc) and handles the pickling appropriately.
    #     """
    #     if _cloudpickle._should_pickle_by_reference(obj, name=name):
    #         return _pickle.Pickler.save_global(self, obj, name=name)
    #     elif PYPY and isinstance(obj.__code__, builtin_code_type):
    #         return self.save_pypy_builtin_func(obj)
    #     else:
    #         return _save_reduce_pickle5(
    #             self,
    #             *self._dynamic_function_reduce(obj),
    #             obj=obj,
    #         )

    # def save_pypy_builtin_func(self, obj):
    #     """Save pypy equivalent of builtin functions.

    #     PyPy does not have the concept of builtin-functions. Instead,
    #     builtin-functions are simple function instances, but with a
    #     builtin-code attribute.
    #     Most of the time, builtin functions should be pickled by attribute.
    #     But PyPy has flaky support for __qualname__, so some builtin
    #     functions such as float.__new__ will be classified as dynamic. For
    #     this reason only, we created this special routine. Because
    #     builtin-functions are not expected to have closure or globals,
    #     there is no additional hack (compared the one already implemented
    #     in pickle) to protect ourselves from reference cycles. A simple
    #     (reconstructor, newargs, obj.__dict__) tuple is save_reduced.  Note
    #     also that PyPy improved their support for __qualname__ in v3.6, so
    #     this routing should be removed when _cloudpickle supports only PyPy
    #     3.6 and later.
    #     """
    #     rv = (
    #         _cloudpickle.types.FunctionType,
    #         (obj.__code__, {}, obj.__name__, obj.__defaults__, obj.__closure__),
    #         obj.__dict__,
    #     )
    #     self.save_reduce(*rv, obj=obj)

    # _cloudpickle.Pickler.dispatch[_cloudpickle.types.FunctionType] = save_function

elif BASE_PICKLE == "pickle":
    pass

else:
    raise ValueError(f"Invalid BASE_PICKLE= {BASE_PICKLE}")
