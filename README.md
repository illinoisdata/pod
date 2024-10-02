# Chipmink: Partial Object Store for Temporal Exploration on Notebooks

At the core to enable notebook version control, environment migrations, and collaborative data analysis, we need a new type of storage to store and retrieve Python variables efficiently. It must be able to manage many variable versions as the number of notebook cell executions and gracefully handle variable dependency with potentially shared and/or unserializable objects. Existing solutions like ad hoc saving/loading, snapshotting, or restart/re-execution are incomplete, inaccurate, or unscalable.

Instead, we aim to develop a new *partial object store* that correctly groups Python objects into pods and detects their changes at pod granularity (historically "pod" stands for partially orered delta).

*Currently this is a research prototype. If you'd like us to productionize it, please let us know (e.g., posting a feature request)!*


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

ns3 = pod.load(t3)
ns3["x"] = ns3["x"][-1:]
ns3["y"].clear()
ns3["y"].append("2")
print(ns3)  # Change reflects in both "x" and "y"
```

## Reproducibility

All experiments in this work are performed through Docker environment.

Download and extract notebook's dataset.
```bash
curl -L https://uofi.box.com/shared/static/ht62bhigo99w1unxl4xazuj74bzrrqre --output poddata.zip
unzip poddata.zip
mkdir -p nbdata/data
mv poddata/* nbdata/data/
```

Start up a Docker compose on a terminal.
```bash
bash experiments/start-all.sh pod_${USER} $(pwd)/nbdata
```

On a separate terminal, run all the experiments. Measurements will be collected under respective folders in `results`. For smaller testbeds, the set of notebooks can be changed to `ai4code,twittnet,skltweet,betterxb,tpsmay22` and vice versa.
```bash
export output=log.txt
(docker compose -f experiments/docker-compose.yml -p pod_${USER} exec podnogil bash experiments/bench_exp1.sh snp,dill,shev,zosp,zodb,criu,crii,pga,pfl,pfa,pg0,pg1,pgcache0,pgcache0noavf,pgcache0,pgcache4,pgcache16,pgcache64,pgcache256,pgcache1024,pgcache4096,pgcache16384,pgcache65536,pgcache262144,pgcache1048576,pgl,pglnoavlstatic,pglnostatic,pgnoavf,pgnoavl,pgnoavlstatic,pgnostatic,pnb,pnv,prand skltweet,ai4code,agripred,msciedaw,ecomsmph) 2>& 1 | tee -a $output

# Selective Saving
(docker compose -f experiments/docker-compose.yml -p pod_${USER} exec podnogil bash experiments/bench_exp1.sh snp,shev,zosp,zodb,criu,crii,pga skltweet[exc],ai4code[exc],agripred[exc],msciedaw[exc],ecomsmph[exc]) 2>& 1 | tee -a $output

# A Case for Object-aware Deltas
(docker compose -f experiments/docker-compose.yml -p pod_${USER} exec podnogil bash experiments/bench_exp1.sh snz,pgaz skltweet) 2>& 1 | tee -a $output
(docker compose -f experiments/docker-compose.yml -p pod_${USER} exec pod39 python pod/bench.py exp1 --expname exp1_snz_skltweet --nbname skltweet --nb notebooks/sklearn_tweet_classification.ipynb --sut snapshotzlib --pod_dir /tmp/pod) 2>& 1 | tee -a $output  # xdelta is no longer maintained for Python>=3.10

# Chipmink Captures Partial Changes
(docker compose -f experiments/docker-compose.yml -p pod_${USER} exec podnogil bash experiments/bench_exp2.sh snp,dill,shev,zosp,zodb,criu,crii,pga 10 1e2 1e5 0,10,20,30,40,50,60,70,80,90,100 100) 2>& 1 | tee -a $output

# Chipmink Scales with Data Sizes
(docker compose -f experiments/docker-compose.yml -p pod_${USER} exec podnogil bash experiments/bench_exp2.sh snp,dill,shev,zosp,zodb,criu,crii,pga 2 1,2,3 1,2,3 1,10,100 1,10,100) 2>& 1 | tee -a $output
(docker compose -f experiments/docker-compose.yml -p pod_${USER} exec podnogil bash experiments/bench_exp2.sh snp,dill,shev,zosp,zodb,criu,crii,pga 10 1e2 1e0,1e1,1e2,1e3,1e4,1e5,1e6 1 100) 2>& 1 | tee -a $output

# Access Control Overhead
docker compose -f experiments/docker-compose.yml -p pod_${USER} exec podnogil bash experiments/bench_exp1.sh noop,pgnoop skltweet[n=0],ai4code[n=0],agripred[n=0],msciedaw[n=0],ecomsmph[n=0],skltweet[n=1],ai4code[n=1],agripred[n=1],msciedaw[n=1],ecomsmph[n=1],skltweet[n=2],ai4code[n=2],agripred[n=2],msciedaw[n=2],ecomsmph[n=2],skltweet[n=3],ai4code[n=3],agripred[n=3],msciedaw[n=3],ecomsmph[n=3],skltweet[n=4],ai4code[n=4],agripred[n=4],msciedaw[n=4],ecomsmph[n=4],skltweet[n=5],ai4code[n=5],agripred[n=5],msciedaw[n=5],ecomsmph[n=5],skltweet[n=6],ai4code[n=6],agripred[n=6],msciedaw[n=6],ecomsmph[n=6],skltweet[n=7],ai4code[n=7],agripred[n=7],msciedaw[n=7],ecomsmph[n=7],skltweet[n=8],ai4code[n=8],agripred[n=8],msciedaw[n=8],ecomsmph[n=8],skltweet[n=9],ai4code[n=9],agripred[n=9],msciedaw[n=9],ecomsmph[n=9]) 2>& 1 | tee -a $output

unset output
```

Next, run `plot.ipynb` to format the results.

Finally, compile the plots using the following command.
```bash
(cd plots && pdflatex plot.tex && open plot.pdf)
```

## Development

Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.

This repository is scaffolded based on https://github.com/rochacbruno/python-project-template.
