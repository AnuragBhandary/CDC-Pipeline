"""Benchmark: replay N row changes through the whole pipeline and measure it.

    uv run python bench/run_benchmark.py --events 1000000 --rate 4000 --sinks 3 \
        --schema-change-at 500000 --kill-at 700000 --label resume

Each run is isolated: its own topic prefix, connector, consumer group and target schema, on a
freshly reset source.

1. seed the source (rows arrive as Debezium snapshot events), start the connector and N sinks
2. generate the workload at a fixed rate; optionally
     - ALTER TABLE customers ADD COLUMN at --schema-change-at events (no pause, no restart)
     - SIGKILL one sink at --kill-at events, while it holds an open transaction, and restart it
       --restart-after seconds later
3. wait until the sinks have caught up, compare every table (row by row + SHA-256)
4. report replication lag (source commit -> PostgreSQL commit, every event) and counts

Lag is measured by the sinks themselves: Debezium's source.ts_us is the MySQL commit time with
microsecond precision, and each sink records (its PostgreSQL commit time - ts_us) for every
event into a histogram flushed to _cdc.consumer_stats. Same machine, same clock.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path

from cdcpipe import connector, verify, workload
from cdcpipe.config import Settings

OUT = Path(__file__).parent / "out"


def pct(hist: Counter[int], q: float) -> int:
    total = sum(hist.values())
    target = q * total
    seen = 0
    for ms in sorted(hist):
        seen += hist[ms]
        if seen >= target:
            return ms
    return max(hist) if hist else 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=1_000_000)
    ap.add_argument("--rate", type=float, default=4000)
    ap.add_argument("--writers", type=int, default=8)
    ap.add_argument("--sinks", type=int, default=3)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--customers", type=int, default=20_000)
    ap.add_argument("--schema-change-at", type=int)
    ap.add_argument("--kill-at", type=int)
    ap.add_argument("--restart-after", type=float, default=2.0)
    ap.add_argument("--label", default="run")
    a = ap.parse_args()

    run_id = f"{a.label}{int(time.time()) % 100000}"
    base = Settings.from_env()
    s = replace(base, topic_prefix=f"b{run_id}", target_schema=f"shop_{run_id}")
    group = f"g{run_id}"
    env = {**os.environ, "TOPIC_PREFIX": s.topic_prefix, "TARGET_SCHEMA": s.target_schema}
    print(f"run {run_id}: prefix {s.topic_prefix}, schema {s.target_schema}, group {group}")

    connector.delete(base)  # any connector left on the default prefix
    workload.reset(s)
    seeded = workload.seed(s, customers=a.customers)
    connector.create(s, server_id=6000 + int(time.time()) % 1000)

    def start_sink(i: int) -> subprocess.Popen[bytes]:
        log = (OUT / f"{run_id}-sink-{i}.log").open("ab")
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "cdcpipe",
                "sink",
                "--instance",
                f"sink-{i}",
                "--group",
                group,
                "--run-id",
                run_id,
                "--batch",
                str(a.batch),
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    OUT.mkdir(exist_ok=True)
    sinks = {i: start_sink(i) for i in range(1, a.sinks + 1)}
    kill_info: dict[str, object] = {}
    state = {"killed": False}

    def progress(n: int, elapsed: float) -> None:
        for i, p in sinks.items():
            if p.poll() is not None:
                raise RuntimeError(f"sink-{i} exited unexpectedly ({p.returncode}); see its log")
        if a.kill_at and not state["killed"] and n >= a.kill_at:
            state["killed"] = True
            victim = 2 if a.sinks >= 2 else 1
            with s.pg(autocommit=True) as pg:
                st = pg.execute(
                    "SELECT state, left(query, 60) FROM pg_stat_activity"
                    " WHERE application_name = %s",
                    (f"sink-{victim}",),
                ).fetchall()
            os.kill(sinks[victim].pid, signal.SIGKILL)
            sinks[victim].wait()
            kill_info.update(
                at_events=n,
                at_s=round(elapsed, 1),
                victim=f"sink-{victim}",
                victim_pg_backend=[list(r) for r in st],
            )
            print(f"SIGKILL sink-{victim} at {n:,} events; its PostgreSQL backend was {st}")
            time.sleep(a.restart_after)
            sinks[victim] = start_sink(victim)
            print(f"restarted sink-{victim} after {a.restart_after}s")

    load = workload.run(
        s, a.events, a.rate, a.writers, schema_change_at=a.schema_change_at, progress=progress
    )
    print("load:", load)
    waited = verify.wait_caught_up(s, group, timeout_s=600)
    reports = verify.compare(s)
    for i, p in sinks.items():
        if p.poll() is not None:
            raise RuntimeError(f"sink-{i} exited unexpectedly ({p.returncode}); see its log")
        p.send_signal(signal.SIGTERM)
    for p in sinks.values():
        p.wait(timeout=30)

    with s.pg(autocommit=True) as pg:
        rows = pg.execute(
            "SELECT consumer_id, pid, received, deduped, applied, stale, ops, lag_ms_hist"
            " FROM _cdc.consumer_stats WHERE run_id = %s",
            (run_id,),
        ).fetchall()
        added = pg.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s"
            " AND table_name = 'customers' AND column_name = 'loyalty_tier'",
            (s.target_schema,),
        ).fetchall()
    hist: Counter[int] = Counter()
    ops: Counter[str] = Counter()
    totals = Counter()
    for _cid, _pid, rec, ded, app, stale, o, h in rows:
        hist.update({int(k): v for k, v in h.items()})
        ops.update(o)
        totals.update(received=rec, deduped=ded, applied=app, stale=stale)

    result = {
        "run_id": run_id,
        "params": vars(a),
        "seeded_rows_snapshot": seeded,
        "load": load,
        "kill": kill_info or None,
        "events_by_op": dict(ops),
        "sink_totals": dict(totals),
        "lag_ms": {
            "p50": pct(hist, 0.5),
            "p95": pct(hist, 0.95),
            "p99": pct(hist, 0.99),
            "p999": pct(hist, 0.999),
            "max": max(hist) if hist else None,
            "events_measured": sum(hist.values()),
        },
        "caught_up_after_marker_s": round(waited, 1),
        "schema_change_column_in_target": bool(added),
        "verify": [{**vars(r), "ok": r.ok} for r in reports],
        "all_tables_match": all(r.ok for r in reports),
    }
    (OUT / f"{run_id}.json").write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps({k: v for k, v in result.items() if k != "verify"}, indent=2, default=str))
    for r in reports:
        print(
            f"{'ok  ' if r.ok else 'FAIL'} {r.table:12s} {r.source_rows:>8,} rows"
            f"  missing {r.missing} extra {r.extra} mismatched {r.mismatched}"
            f"  sha256 {r.source_sha256[:16]} {'=' if r.ok else '!='} {r.target_sha256[:16]}"
        )
    sys.exit(0 if result["all_tables_match"] else 1)


if __name__ == "__main__":
    main()
