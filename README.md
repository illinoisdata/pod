# Pod: A Storage for Versioned and Dependent Variables

At the core to enable notebook version control, environment migrations, and collaborative data analysis, we need a new type of storage to store and retrieve Python variables efficiently. It must be able to manage many variable versions as the number of notebook cell executions and gracefully handle variable dependency with potentially shared and/or unserializable objects. Existing solutions like ad hoc saving/loading, snapshotting, or restart/re-execution are incomplete, inaccurate, or unscalable.

Instead, we aim to develop a new conceptual data model—partially ordered deltas—that correctly represents changing Python variables. Leveraging the sparsity and correlation in these variable changes, Pod implements an optimized data layout as well as an adaptive data movement protocol that minimizes I/O and storage costs.

<!-- ## Install it from PyPI

```bash
pip install pod
``` -->

## Usage

```py
import pod

# Save namespace, potentially with shared objects.
shared_buffer = [0, 0, 0, 0]
namespace = {"x": 1, "y": [2, shared_buffer], "z": [3, shared_buffer]}
pod.save(namespace)

# Namespace mutates, Pod only saves mutated part.
namespace["y"][0] = 22
pod.save(namespace)
```

<!-- ```bash
$ python -m pod
#or
$ pod
``` -->

## Development

Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.

This repository is scaffolded based on https://github.com/rochacbruno/python-project-template.
