#include <Python.h>
#include <unordered_map>
#include <vector>

// Define structures
typedef struct {
    size_t tid;
    size_t oid;
} PodId;

// Define equality comparison for PodId
bool operator==(const PodId& lhs, const PodId& rhs) {
    return lhs.tid == rhs.tid && lhs.oid == rhs.oid;
}

// Define a hash function for PodId
struct PodIdHash {
    std::size_t operator()(const PodId& pid) const {
        auto h = std::hash<size_t>();
        std::size_t hash_tid = h(pid.tid);
        std::size_t hash_oid = h(pid.oid);
        return hash_tid ^ (hash_oid << 1); // Combine tid and oid hashes
    }
};

// Define an equality comparison function for PodId
struct PodIdEqual {
    bool operator()(const PodId& lhs, const PodId& rhs) const {
        return lhs.tid == rhs.tid && lhs.oid == rhs.oid;
    }
};

typedef std::unordered_map<PodId, PodId, PodIdHash, PodIdEqual> RootsDict;

// Declare find_root function
PodId find_root(PodId pid, RootsDict& roots_dict);

// Union find with path compression
static PyObject* union_find(PyObject *self, PyObject *args) {
    PyObject *deps_dict;
    if (!PyArg_ParseTuple(args, "O", &deps_dict)) {
        PyErr_SetString(PyExc_TypeError, "Expected pod dependency dictionary");
        return NULL;
    }

    RootsDict roots_dict;

    PyObject *deps_keys = PyDict_Keys(deps_dict);
    Py_ssize_t num_deps = PyList_Size(deps_keys);

    // fprintf(stderr, "Iterating %ld items\n", num_deps);

    // Union find iterations
    std::vector<PodId> all_pids;
    for (Py_ssize_t i = 0; i < num_deps; i++) {
        PyObject *pid_key = PyList_GetItem(deps_keys, i);
        PyObject *dep = PyDict_GetItem(deps_dict, pid_key);

        PodId pid;
        pid.tid = PyLong_AsLong(PyObject_GetAttrString(pid_key, "tid"));
        pid.oid = PyLong_AsLong(PyObject_GetAttrString(pid_key, "oid"));
        // fprintf(stderr, "At tid= %ld, oid= %ld\n", pid.tid, pid.oid);
        all_pids.push_back(pid);

        int immutable = PyObject_IsTrue(PyObject_GetAttrString(dep, "immutable"));
        if (immutable) {
            continue;
        }

        PyObject *dep_pids = PyObject_GetAttrString(dep, "dep_pids");

        // Iterate over outgoing edge from this pid.
        PyObject *iter = PyObject_GetIter(dep_pids);
        if (!iter) {
            PyErr_SetString(PyExc_TypeError, "dep_pids is not iterable");
            return NULL;
        }
        PyObject *pid2_key;
        while ((pid2_key = PyIter_Next(iter))) {
            PodId pid2;
            pid2.tid = PyLong_AsLong(PyObject_GetAttrString(pid2_key, "tid"));
            pid2.oid = PyLong_AsLong(PyObject_GetAttrString(pid2_key, "oid"));
            // fprintf(stderr, "  At tid= %ld, oid= %ld\n", pid2.tid, pid2.oid);

            PyObject *dep2 = PyDict_GetItem(deps_dict, pid2_key);
            int immutable2 = PyObject_IsTrue(PyObject_GetAttrString(dep2, "immutable"));
            if (immutable2) {
                continue;
            }
            PodId root_pid = find_root(pid, roots_dict);
            PodId root_pid2 = find_root(pid2, roots_dict);
            if (root_pid.oid == 0 || root_pid2.oid == 0) {
                continue;
            }
            roots_dict[root_pid2] = root_pid;
        }
        Py_DECREF(dep_pids);
    }

    Py_DECREF(deps_keys);

    // Translate roots_dict to a Python dictionary
    // PyObject *py_roots_dict = PyDict_New();
    PyObject *py_roots_dict = PyList_New(0);
    for (const PodId& key : all_pids) {
        PodId value = find_root(key, roots_dict);
        // PyObject *py_key = Py_BuildValue("(KK)", key.tid, key.oid);
        // PyObject *py_value = Py_BuildValue("(KK)", value.tid, value.oid);
        // PyDict_SetItem(py_roots_dict, py_key, py_value);
        // Py_DECREF(py_key);
        // Py_DECREF(py_value);
        PyObject *py_kv = Py_BuildValue("((KK)(KK))", key.tid, key.oid, value.tid, value.oid);
        PyList_Append(py_roots_dict, py_kv);
        Py_DECREF(py_kv);
    }

    return py_roots_dict;
}

// Find root with path compression.
PodId find_root(PodId pid, RootsDict& roots_dict) {
    auto it = roots_dict.find(pid);
    if (it == roots_dict.end() || it->second == pid) {
        return pid;
    }
    return find_root(it->second, roots_dict);
}

// Method mapping
static PyMethodDef methods[] = {
    {"union_find", union_find, METH_VARARGS, "Union find with path compression"},
    {NULL, NULL, 0, NULL} // Sentinel
};

// Module definition
static struct PyModuleDef algo_module = {
    PyModuleDef_HEAD_INIT,
    "algo",
    NULL,
    -1,
    methods
};

// Module initializer
PyMODINIT_FUNC PyInit_algo(void) {
    return PyModule_Create(&algo_module);
}
