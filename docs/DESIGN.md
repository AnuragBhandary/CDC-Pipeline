# Design

## Goal

Keep a PostgreSQL replica of an operational MySQL database continuously in sync, with
**inserts, updates and deletes**, within seconds, and be able to *prove* the replica equals the
source after crashes and schema changes.

## Architecture

```
MySQL 8.4 ──binlog (ROW, FULL images, GTID)──▶ Debezium MySQL connector (Kafka Connect)
                                                   │ one topic per table, 3 partitions,
                                                   │ message key = primary key
                                                   ▼
                                                 Kafka (KRaft)
                                                   │ consumer group, static membership,
                                                   │ cooperative-sticky rebalancing
                                                   ▼
                                     cdcpipe sink × N (Python, confluent-kafka)
                                                   │ batch → dedupe → COPY + guarded MERGE
                                                   ▼
                                     PostgreSQL 16 (replica schema + _cdc.consumer_stats)
```

Why log-based CDC and not polling `updated_at` (which Project 1 does):

| | Polling `updated_at` | Binlog CDC |
|---|---|---|
| Deletes | invisible | captured (with the full before-image) |
| Intermediate states | lost if a row changes twice between polls | every change, in commit order |
| Late commits | needs a lookback window | none: the binlog is in commit order |
| Load on the source | a query per table per poll | reads the log the server writes anyway |
| Latency | the poll interval | ~100 ms here |
| Cost | trivial | Kafka + Connect to operate |

## What the probe showed

Before writing the sink I registered a connector and inspected real events from a transaction
that updated one row twice and inserted two rows in one statement:

| Event | `source.pos` | `source.row` |
|---|---|---|
| update #1, row 1 | 990 | 0 |
| update #2, row 1 (same transaction) | 1134 | 0 |
| multi-row insert, row 2 | 1278 | 0 |
| multi-row insert, row 3 | 1278 | 1 |

So **(binlog file number, pos, row) is strictly increasing**, even inside one transaction, and
works as a per-row version. `source.ts_us` is the MySQL commit time in microseconds, which is
what the lag measurement uses.

## The sink

### Idempotent apply

Every target row stores the position of the event that produced it (`_src_file`, `_src_pos`,
`_src_row`). A batch is applied as

```sql
INSERT INTO shop.t AS t (...) SELECT ... FROM _stage
ON CONFLICT (pk) DO UPDATE SET ... 
WHERE (t._src_file, t._src_pos, t._src_row) < (EXCLUDED._src_file, EXCLUDED._src_pos, EXCLUDED._src_row)
```

The consequences:
- **Replays are no-ops.** After a crash, a rebalance or a Debezium restart, an event that was
  already applied has an equal position and is skipped.
- **Order doesn't matter.** An older event arriving after a newer one can't overwrite it.
- Within a batch, only the newest event per key is staged (fewer writes, and one key per
  `ON CONFLICT` statement, which PostgreSQL requires).

### Delivery: at-least-once plus idempotence

```
consume batch → PostgreSQL BEGIN … MERGE … COMMIT → Kafka commit offsets
```

| Crash point | What happens on restart |
|---|---|
| before the PostgreSQL commit | transaction rolled back; the batch is re-consumed and applied |
| between the PostgreSQL and Kafka commits | the batch is re-consumed; every event is stale, so nothing changes |
| after the Kafka commit | nothing to redo |

Tests kill a sink at each of the first two points (`CDCPIPE_HANG` freezes it there, then SIGKILL)
and check the replica is identical to the source afterwards.

The alternative is exactly-once by storing Kafka offsets in PostgreSQL in the same transaction,
and seeking to them on startup. It's equivalent in effect here, but idempotent apply also
protects against duplicates from *upstream* (Debezium re-emits events after its own restarts),
which offset-in-target alone wouldn't.

### Deletes are soft

A delete event is applied as an upsert of its before-image with `_deleted = true` and the
delete's position. A hard `DELETE` would discard the position, and a replayed older insert would
then resurrect the row. A test does exactly that replay. Views or a periodic purge can hide or
remove soft-deleted rows; the verifier compares only live rows.

### Schema evolution without downtime

Every message carries its Kafka Connect schema. The sink compares it with the target table
(cached, re-read from `pg_catalog` under a per-table advisory lock when something differs):

| Source change | Sink action |
|---|---|
| new column | `ALTER TABLE ADD COLUMN` (nullable), in the same transaction as the batch that first carries it |
| wider DECIMAL | `ALTER COLUMN TYPE` to the new precision |
| incompatible type change | stop with `SchemaConflict`; never guess |
| dropped column | the column stays and is NULL from then on (full row images) |

Because MySQL sends full row images, the merge writes *every* target column. A column the event
doesn't carry is set to NULL. An early version only updated the event's own columns and left a
stale value behind when a table was recreated without that column. Leftover test data exposed it.

In the 1M-event run, `ALTER TABLE customers ADD COLUMN loyalty_tier` ran at event 500,000 while
8 writers and 3 sinks kept going. MySQL 8 does it `ALGORITHM=INSTANT`, Debezium picks the DDL up
from the binlog, and the sink adds the column on the first event that has it.

### Scaling and rebalancing

- Debezium keys every message by primary key, so all changes to one row go to one partition, in
  order. Three partitions per table allow three sinks in parallel.
- **Static membership** (`group.instance.id`): a sink that restarts with the same id gets its
  partitions back immediately instead of waiting out the session timeout.
- **Cooperative-sticky** assignment: when a new table's topic appears, only the new partitions
  move. With the default eager assignor every sink stopped while partitions were reshuffled.
- Bootstrap DDL (schema and stats table) runs under an advisory lock. Three sinks starting
  together once raced on `CREATE SCHEMA IF NOT EXISTS`, which isn't atomic in PostgreSQL.

### Latency tuning

| Setting | Default | Here | Why |
|---|---|---|---|
| Debezium `poll.interval.ms` | 500 | 50 | Debezium waits this long between batches when idle |
| librdkafka `fetch.wait.max.ms` | 500 | 20 | the broker holds a fetch this long waiting for data |
| `topic.metadata.refresh.interval.ms` | 300000 | 1000 | a regex subscription only sees new topics on refresh |
| producer `linger.ms` | 0 | 5 | small batching, compressed with lz4 |

Before the metadata change, the first events of every table created during the run waited for
the next refresh: p99 was 4.5 s even with no failures.

## Verification

`cdcpipe verify`:

1. **Catch-up barrier.** Insert a marker row into MySQL, wait until it's applied, then wait until
   the consumer group has zero lag on every partition. Debezium emits in binlog order, so
   everything before the marker has been produced.
2. **Row-by-row comparison** under a consistent MySQL snapshot. Values are normalized to one text
   form on both sides (exact decimals, microsecond timestamps, a NULL sentinel). Reported per
   table: missing, extra and mismatched rows, plus a **SHA-256 checksum** over the sorted rows.

The benchmark fails if any table differs.

## Known limits

- One MySQL source, so positions compare within one binlog sequence. With failover to a replica,
  positions change servers; GTID-based ordering would be needed.
- `TRUNCATE` events aren't supported (the sink stops on them).
- Soft-deleted rows accumulate; production would purge them after a retention period.
- The replica is eventually consistent *per row*, not per transaction: a reader can see an order
  before its items land, because they're on different partitions. Debezium's transaction
  metadata topic could be used to apply whole transactions atomically.
