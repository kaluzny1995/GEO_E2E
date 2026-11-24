# Memory leaks investigation 2026-11-24

## Memory leaks investigation 2026-11-24

### Symptom

The host was at **33 % memory before the 2026-11-24 pipeline run and 35.2 %
after it** — about **+1.4 GiB** out of 62.5 GiB. Yesterday's investigation had
cleared the GEO stack of a multi-day leak, so this follow-up checks whether the
daily run left memory behind.

### Finding: desktop growth, not the pipeline

A snapshot at 18:30 on 2026-11-24:

| Consumer | 2026-11-23 22:14 | 2026-11-24 18:30 | Change |
|----------|-----------------:|-----------------:|-------:|
| `gnome-system-monitor` | 9.78 GiB | **10.65 GiB** | **+0.87 GiB** |
| Chrome (all processes) | 6.33 GiB | 6.54 GiB | +0.21 GiB |
| Nine GEO containers (`docker stats` total) | — | **1.95 GiB** | flat / lower |
| `free` used | — | 22.0 GiB (35.2 %) | — |

The two desktop processes alone grew by about **1.1 GiB**, which is most of the
observed +1.4 GiB. `gnome-system-monitor` was sampled twice over three minutes
and grew by ~2 MiB in 163 seconds — the same **~1.1 GiB/day** linear growth as
yesterday.

`free` also showed 30.0 GiB of page cache and 40.5 GiB available at the same
time. The monitor's percentage moves with cache and desktop applications, so a
2-point change cannot be used as a stack leak signal by itself.

### The 2026-11-24 pipeline run

Task-log memory lines from today's run:

| Time | Host used |
|------|----------:|
| 07:00:18 | 21.43 GiB |
| 07:22:12 | 21.57 GiB |

That is **+0.14 GiB during the 22-minute run**, inside the normal host
fluctuation. After the run:

* scheduler cgroup current **507.8 MiB** (peak 2.45 GiB during ETL);
* Spark worker 1 **255.3 MiB**, worker 2 **267.9 MiB**;
* API **105.4 MiB**, Streamlit **103.4 MiB**, DAG processor **207.8 MiB**;
* no live `conda run`, `run_etl`, `run_download` or PySpark driver process in
  the scheduler container.

The nine GEO containers together held **1.95 GiB**, below their pre-run
footprint and far below their Compose limits. The run left no memory behind.

### Change made

No repository leak was found, so no functional fix was needed today. The task
callback now also records the host's **available and cached** memory in the
`Memory breakdown:` line:

```
NIFC.download Memory: 22.00 out of 62.50 GiB (35.20 %) used
NIFC.download Memory breakdown: 148.3 MiB RSS, 0.0 MiB children; cgroup 579.0/4096.0 MiB (peak 2454.9 MiB); host available 40.5 GiB, cache 30.0 GiB
```

This makes a cache-driven percentage change distinguishable from a process or
container leak without leaving the Airflow UI. `get_host_memory_detail()` and
the extended breakdown formatting are covered in `test/test_util.py`.

### Deployment

The `geo_airflow_spark` image was rebuilt with `./build-images.sh` and the stack
was recreated with `./up.sh -d`. All nine services reported healthy, and the new
breakdown line was verified inside the scheduler container.

### Actual remediation

The leak is the desktop monitor: **close or restart `gnome-system-monitor`** to
release its ~10.6 GiB history buffer. For unattended monitoring use a bounded
logger (`psutil`/`vmstat`) instead of leaving the GUI open. Chrome accounts for
most of the remaining desktop memory.

### Confirmation

At about 18:44 the `gnome-system-monitor` process was no longer running, and
host usage dropped from **22.0 GiB (35.2 %)** to **11.1 GiB (17.6 %)** — almost
exactly the monitor's 10.65 GiB RSS. That directly confirms the attribution:
closing the system monitor released the memory; no GEO container or task process
was stopped to achieve it.

### Verification

```bash
cd GEO_E2E
conda run -n geo_e2e_311 python -m pytest -q

# host vs containers, outside the task log
free -m
docker stats --no-stream
ps -eo pid,rss,comm --sort=-rss | head -15
```
