# Memory leaks investigation 2026-11-23

## Memory leaks investigation 2026-11-23

### Symptom

The stack had been up since 2026-11-17 without a task error. The host memory
recorded by the `on_success` / `on_failure` callbacks nonetheless rose steadily:

| Date | Range during the run | Baseline after the run |
|------|---------------------:|-----------------------:|
| 2026-11-17 | 11.92–14.47 GiB | 11.99 GiB |
| 2026-11-18 | 13.25–15.46 GiB | 13.25 GiB |
| 2026-11-19 | 14.61–16.94 GiB | 14.61 GiB |
| 2026-11-20 | 15.89–18.28 GiB | 15.93 GiB |
| 2026-11-21 | 17.10–19.72 GiB | 17.28 GiB |
| 2026-11-22 | 18.57–20.64 GiB | 18.60 GiB |
| 2026-11-23 | 19.63–21.83 GiB | 19.63 GiB |

That is roughly **1.3 GiB/day** on a 62.5 GiB host. The user-reported span was
8.7 → 22.2 GiB over the whole run.

### Root cause: host desktop, not the GEO containers

The number above is `psutil.virtual_memory().used` of the **whole host**, not of
the GEO stack. A snapshot on 2026-11-23 attributed it as follows:

| Consumer | Memory |
|----------|--------|
| `gnome-system-monitor` (one process, up since boot) | **9.74 GiB RSS** |
| Chrome (all renderers) | ~6.3 GiB |
| All nine GEO containers (cgroup current incl. page cache) | ~3.0–3.4 GiB |
| All nine GEO containers (`docker stats`, cache excluded) | ~2.6 GiB |

`gnome-system-monitor` had a linear growth of **~12–15 KiB/s ≈ 1.1 GiB/day**
(measured over two 2-minute samples). That single desktop process accounts for
almost all of the 1.3 GiB/day baseline growth; Chrome explains the rest of the
host total. The GEO containers were bounded by Compose and did not grow:
the scheduler cgroup current stayed at ~0.7–1.0 GiB (peak 2.9 GiB during an ETL
run), each Spark worker at ~0.3–0.4 GiB (peak 2.3–2.4 GiB), the API at ~0.2 GiB
and Streamlit at ~0.35 GiB.

Reproduce the attribution on the host:

```bash
# Host total and the top consumers
free -h
ps -eo pid,rss,comm --sort=-rss | head -15

# Containers only
docker stats --no-stream

# Per-process growth while the stack is idle
for i in 1 2 3; do date +%T; ps -o pid,rss= -C gnome-system-monitor; sleep 60; done
```

If the host total rises while the `docker stats` total is flat, the leak is
outside the stack. `gnome-system-monitor` keeps its history graph in memory;
closing it (or restarting it daily) releases the growth. For unattended hosts,
use `psutil`/`vmstat` logging instead of leaving the GUI open.

### GEO_E2E audit

* The DAG module caches only immutable configuration (`@lru_cache(maxsize=1)` on
  `config.json` / `jobs.json`); no DAG-run data is retained.
* Airflow task subprocesses (`conda run …`) exit after each step, so their
  PySpark drivers and DataFrame caches cannot accumulate across days.
* All containers have `mem_limit` / `memswap_limit`; Docker json-file logs are
  capped at 10 MiB × 3, so log growth cannot exhaust RAM.
* The scheduler/worker cgroups did not grow across the seven days, despite the
  host baseline rising.

No leak was found in the orchestrator code.

### Change made

The old callback logged only the host total, which is exactly what made this
look like a GEO leak. The task log now also gets a **process/container
breakdown**:

```
NIFC.download Memory: 19.63 out of 62.50 GiB (31.41 %) used
NIFC.download Memory breakdown: 201.4 MiB RSS, 612.8 MiB children; cgroup 702.1/4096.0 MiB (peak 2048.0 MiB); host available 40.5 GiB, cache 30.0 GiB
```

* `get_process_memory()` returns RSS of the callback process and its recursive
  children (the LocalExecutor workers and the running task).
* `get_cgroup_memory()` reads cgroup v2 `memory.current` / `memory.peak` /
  `memory.max`.
* `get_host_memory_detail()` reports the host's available and cached memory, so
  a percent change caused by page cache is not mistaken for process growth.
* `log_memory_breakdown()` skips unavailable sources instead of failing the
  callback.

Future reports can now distinguish a task/driver leak (process/children grow)
from a container leak (cgroup grows) from page-cache fluctuation or an
unrelated host process (only the host total/available/cache move). Tests cover
the formatting, the unlimited-cgroup case and the missing-cgroup fallback in
`test/test_util.py`.

### Deployment

After the code and configuration changes, all four images were rebuilt serially with
`./build-images.sh` and the stack was recreated with `./up.sh -d`. The nine
services reported healthy, and the new `Memory breakdown:` line, API pool settings
and Streamlit provider-close code were verified inside the recreated containers.

### Verification

```bash
cd GEO_E2E
conda run -n geo_e2e_311 python -m pytest -q

# the next task log line now carries both the host total and the breakdown
docker compose logs airflow-scheduler | grep 'Memory breakdown'
```

### Conclusion

The 17–23 November growth was the host desktop (`gnome-system-monitor`,
secondarily Chrome), not Airflow, Spark or the GEO code. The stack itself stayed
within its Compose memory bounds. The new breakdown turns this diagnosis into a
one-line check on every future task run.
