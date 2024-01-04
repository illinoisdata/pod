# Benchmark Readme

All scripts run from the repository directory root.

## bench_exp1

On tty1:
```bash
 bash experiments/start-all.s
```

On tty2:
```bash
docker compose -f experiments/docker-compose.yml exec pod bash experiments/bench_exp1.sh
```

On tty1:
```bash
 bash experiments/clear-all.s
```
