# Interview guide

How to learn this codebase, pitch it, and defend it. Numbers are in [RESULTS.md](RESULTS.md);
reasoning is in [DESIGN.md](DESIGN.md).

## 1. Learning path (read in this order)

| # | File | What to be able to explain afterwards |
|---|---|---|
| 1 | `docker-compose.yml`, `docker/mysql/init.sql` | Why ROW binlog, FULL row images, GTIDs; the Debezium user's grants; KRaft; why auto-create topics is off |
| 2 | `src/cdcpipe/connector.py` | Every connector setting, especially `snapshot.mode`, `decimal.handling.mode`, `poll.interval.ms`; why `tasks.max = 1`; the offsets API |
| 3 | `src/cdcpipe/decode.py` | The envelope (before/after/op/source); Connect logical types; why (file, pos, row) is a version |
| 4 | `src/cdcpipe/sink.py` → `Applier` | Dedupe, COPY + guarded MERGE, soft delete, schema evolution under an advisory lock |
| 5 | `src/cdcpipe/sink.py` → `run` | Commit order (PostgreSQL, then Kafka); static membership; cooperative-sticky; the hang hooks |
| 6 | `src/cdcpipe/verify.py` | The catch-up barrier; normalization; what missing / extra / mismatched each mean |
| 7 | `src/cdcpipe/workload.py` | Ops and weights; why writers are processes; lock ordering to avoid deadlocks |
| 8 | `tests/test_applier.py` | One test per guarantee: replay, reorder, resurrection, schema change |
| 9 | `tests/test_pipeline.py` | The two crash points and what each assertion proves |
| 10 | `bench/run_benchmark.py` | How lag is measured and why it's trustworthy |

## 2. The 60-second pitch

> "I built change data capture from MySQL to PostgreSQL. Debezium reads the MySQL binlog and
> publishes every insert, update and delete to Kafka, keyed by primary key so each row's changes
> stay in order on one partition. I wrote the sink in Python. It consumes in batches, keeps only
> the newest event per key, and merges into PostgreSQL with a guard: a row is only overwritten
> by an event with a higher binlog position. So replays after a crash are no-ops, and it commits
> Postgres before Kafka offsets, which makes it at-least-once with idempotent apply. Deletes are
> soft so a replayed insert can't resurrect a row. When a column is added in MySQL, the sink sees
> it in the event schema and alters the target table on the fly, with no restart. I replayed a
> million changes at about 3,100 per second with three sinks: p95 lag from MySQL commit to
> Postgres commit was about 140 ms. I added a column halfway, killed a sink, and a row-by-row
> checksum of every table matched at the end."

## 3. Numbers to know cold

See [RESULTS.md](RESULTS.md): events, rate, p50/p95/p99 lag, the kill window's max lag, rows
per table, and that every checksum matched.

## 4. Likely questions

**"Exactly-once?"** End-to-end *effect* is exactly-once; delivery is at-least-once. Kafka
delivers a batch again if the sink dies before committing offsets; the position guard makes the
second application a no-op. Debezium itself is at-least-once too (it re-emits after a restart),
which is why idempotence in the sink matters more than transactional offsets.

**"Why commit PostgreSQL before Kafka?"** The other order loses data: offsets committed, then a
crash before the database commit, and the batch is never seen again.

**"How do you keep per-row ordering with several consumers?"** Debezium keys messages by primary
key, and Kafka keeps order within a partition. One row's changes always go to one partition,
which one consumer owns at a time. Across rows there's no ordering guarantee, and none is needed.
For safety the guard rejects older positions even if something did arrive out of order.

**"Why soft deletes?"** A hard delete removes the position. If the insert is replayed later, it
looks new and the row comes back. A test does exactly that.

**"A column was added in MySQL. What happens?"** MySQL 8 adds it instantly; Debezium reads the
DDL from the binlog and includes the column in later events' schemas. The sink sees an unknown
field, takes a per-table advisory lock, re-reads the catalog, and adds the column in the same
transaction as the batch. Writers and sinks never stop.

**"What if a column type changes?"** Widening a DECIMAL is followed. An incompatible change stops
the sink with an error. A replica with silently wrong types is worse than a paused one.

**"How did you measure lag, and is it honest?"** Debezium's `source.ts_us` is the MySQL commit
time in microseconds. Each sink records PostgreSQL-commit-time minus that, for every event, in a
histogram. It's the same machine and clock. It includes Debezium, Kafka, batching and the
database write: the full path.

**"What did you get wrong at first?"** (pick two)
- Two of three sinks crashed on startup with "unknown topic" (a regex subscription before the
  topics existed). The benchmark still passed, because one sink did all the work. I only noticed
  because the kill hit a dead process. Now an unexpected sink exit fails the benchmark.
- p99 lag was 4.5 s with no failures at all: new tables' topics were only discovered on the
  default 5 s metadata refresh, and each new topic caused a stop-the-world rebalance. Fixed with a
  1 s refresh and the cooperative-sticky assignor; p99 went to under 0.5 s.
- The merge originally updated only the event's own columns, which left stale values when a
  table lost a column. With full row images, every target column has to be written.

**"How would this run in production?"** Replicated Kafka (RF 3) and Connect in distributed mode;
Avro or Protobuf with a schema registry instead of JSON with inline schemas; sinks in a
container orchestrator with the instance id from the pod name; alerts on consumer lag and on
`_cdc.consumer_stats`; a nightly verify on a replica snapshot; purging soft-deleted rows.

**"Why not Kafka Connect's JDBC sink?"** It would work for the basic case. Writing the sink
shows the parts you'd otherwise have to trust: idempotence on source position, delete semantics,
and schema evolution with a lock. In production I'd evaluate the JDBC or Debezium JDBC sink first.
