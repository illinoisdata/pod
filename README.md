# Chipmink: Partial Object Store for Temporal Exploration on Notebooks

At the core to enable notebook version control, environment migrations, and collaborative data analysis, we need a new type of storage to store and retrieve Python variables efficiently. It must be able to manage many variable versions as the number of notebook cell executions and gracefully handle variable dependency with potentially shared and/or unserializable objects. Existing solutions like ad hoc saving/loading, snapshotting, or restart/re-execution are incomplete, inaccurate, or unscalable.

Instead, we aim to develop a new *partial object store* that correctly groups Python objects into pods and detects their changes at pod granularity (historically "pod" stands for partially orered delta).

*Currently this is a research prototype. If you'd like us to productionize it, please let us know (e.g., posting a feature request)*


## Quick Start

```python
import pod
ns = pod.new_managed_namespace()  # Or replace the global namespace with this namespace.
ns["x"] = ["0" * 1000000]
t1 = pod.save(ns)

ns["y"] = ["1" * 1000000]
t2 = pod.save(ns)

ns["x"].append(ns["y"])
t3 = pod.save(ns)

print(pod.load(t1), pod.load(t2), pod.load(t3))
```

## Reproducibility

All experiments in this work are performed through Docker environment.

Download notebook dataset.
```bash
# TBD: Link and instruction to download dataset
```

Start up a Docker compose on a terminal.
```bash
bash experiments/start-all.sh pod_${USER} $(pwd)/nbdata
```

On a separate terminal, run all the experiments.
```bash
export output=log.txt
(docker compose -f experiments/docker-compose.yml -p pod_${USER} exec podnogil bash experiments/bench_exp1.sh snp,shev,zosp,zodb,pga,pfl,pfa,pg0,pg1,pgcache0,pgcache0noavf,pgcache0,pgcache4,pgcache16,pgcache64,pgcache256,pgcache1024,pgcache4096,pgcache16384,pgcache65536,pgcache262144,pgcache1048576,pgl,pglnoavlstatic,pglnostatic,pgnoavf,pgnoavl,pgnoavlstatic,pgnostatic,pnb,pnv,prand ai4code,twittnet,skltweet,betterxb,tpsmay22) 2>& 1 | tee -a $output
unset output
```

Measurements will be collected under respective folders in `results`. `plot.ipynb` contains some visualizations and code to format these results for PGFPlots.


## Development

Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.

This repository is scaffolded based on https://github.com/rochacbruno/python-project-template.
