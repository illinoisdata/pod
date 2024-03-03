import pickle
import sys

# Use Python pickle for testing pod prototype. When ideas are solidified and pod is implemented in C, revert this back.
pickle.Pickler = pickle._Pickler  # type: ignore
pickle.Unpickler = pickle._Unpickler  # type: ignore
pickle.dump = pickle._dump  # type: ignore
pickle.dumps = pickle._dumps  # type: ignore
pickle.load = pickle._load  # type: ignore
pickle.loads = pickle._loads  # type: ignore


sys.setrecursionlimit(100000)
