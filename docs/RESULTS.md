# Results

`make bench` ([bench/run_benchmark.py](../bench/run_benchmark.py)) on an Apple M5 Pro laptop,
with everything in Docker. Raw output: [results-1m.json](results-1m.json).

## The 1M-event run

| | |
|---|---|
| Source changes streamed | **1,000,021** row changes: 436,141 inserts, 542,901 updates, 20,357 deletes |
| Plus initial snapshot | 24,001 rows (Debezium `op = r`) |
| Write rate | 3,103 changes/s for 322 s (8 writer processes) |
| Pipeline | MySQL 8.4 → Debezium 3.0 → Kafka 3.9 (3 partitions per table) → **3 sinks** → PostgreSQL 16 |
| Live schema change | `ALTER TABLE customers ADD COLUMN loyalty_tier` at event 500,000; writers and sinks never paused |
| Failure | sink-2 `SIGKILL`ed at event 700,033, restarted 2 s later |

### Replication lag (MySQL commit → PostgreSQL commit, every streamed event)

| p50 | p95 | p99 | p99.9 | max |
|---|---|---|---|---|
| **89 ms** | **141 ms** | **183 ms** | 2,965 ms | 3,751 ms |

The tail is the kill: events on the dead sink's partitions waited for the 2 s restart and its
rejoin. That's about 0.1% of events. Everything else was under 200 ms.

### Source vs replica

After the catch-up barrier, every table was compared row by row under a consistent MySQL
snapshot, with a SHA-256 checksum over the normalized rows:

| Table | Rows | Missing | Extra | Mismatched | SHA-256 (source = replica) |
|---|---|---|---|---|---|
| order_items | 284,127 | 0 | 0 | 0 | `3847b56250e2192b…` |
| orders | 113,933 | 0 | 0 | 0 | `ea13c8c9477b9325…` |
| customers | 40,000 | 0 | 0 | 0 | `6cbc9e1235352970…` |
| products | 2,000 | 0 | 0 | 0 | `d1c1a3a667ccc0c8…` |
| cdc_markers | 2 | 0 | 0 | 0 | `d2b169890bfc9678…` |

**Zero lost, duplicated or stale rows.** The sinks received 1,023,400 events; 11,276 were
superseded inside their batch by a newer change to the same row, and 1,012,124 were applied.

## Crash recovery (deterministic, in the test suite)

In the benchmark the kill landed between two batches (the victim's PostgreSQL session was idle),
which is the most likely spot by chance. The two dangerous spots are covered by tests that
freeze the sink exactly there (`CDCPIPE_HANG`) and then SIGKILL it:

| Test | Killed at | Proven |
|---|---|---|
| `test_kill_mid_apply_rolls_back` | inside the PostgreSQL transaction, between two tables of one batch (`pg_stat_activity` shows `idle in transaction`) | nothing partial is visible; the restarted sink resumes from the committed Kafka offsets and the replica matches |
| `test_kill_after_postgres_commit_before_kafka_commit` | after the PostgreSQL commit, before the Kafka offset commit | the batch is redelivered; the position guard skips it (stale count > 0); the replica matches |

## Tuning that mattered

| Change | Effect (150k-event runs, 3 sinks, no failures) |
|---|---|
| Baseline | p95 1,927 ms, p99 4,535 ms |
| Seed every table so its topic exists before streaming, 1 s metadata refresh, cooperative-sticky rebalancing | **p95 129 ms, p99 460 ms** |

Without these, the first events of every table created during the run waited for the default
5 s topic-metadata refresh, and each new topic triggered a stop-the-world rebalance across all
sinks.

## Bugs found while building it

1. **Sinks crashed on startup** with "unknown topic" when they subscribed before Debezium had
   created the topics. A calibration run still passed with one surviving sink doing everything.
   It was only noticed because the scheduled kill hit an already-dead process. Fix: treat it as
   transient. The benchmark now fails if any sink exits unexpectedly.
2. **Race on bootstrap DDL**: three sinks starting together collided on `CREATE SCHEMA IF NOT
   EXISTS`, which isn't atomic in PostgreSQL. Fix: an advisory lock.
3. **Stale column values**: the merge only wrote the event's own columns. Leftover data from an
   earlier run (a recreated table without the new column) exposed it. With full row images the
   merge must write every target column.
4. **Type changes went unnoticed**: the schema check compared column *names* only, so a widened
   DECIMAL failed at insert. Found by a unit test; the check now compares types and widens.
