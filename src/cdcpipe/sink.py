"""The PostgreSQL sink: a Kafka consumer that applies Debezium change events idempotently.

Per batch:
  1. consume up to N messages (any tables, any partitions assigned to this consumer)
  2. decode; per table keep only the newest event per primary key (by source position)
  3. one PostgreSQL transaction: create/extend target tables as needed, then for each table
     COPY into a temp table and MERGE with a position guard:
         ON CONFLICT (pk) DO UPDATE ... WHERE target.position < incoming.position
  4. commit PostgreSQL, *then* commit the Kafka offsets

Delivery is at-least-once: a crash between 4a and 4b replays the batch on restart, and the
position guard turns every replayed event into a no-op. A crash before 4a rolls the whole batch
back. Either way the target converges to the source, with nothing applied twice and nothing
lost.

Deletes are soft: the row stays with _deleted = true and the delete's position. A hard delete
would lose the position, so a replayed older insert could resurrect the row.

Schema evolution: every event carries its Connect schema. A field the target table doesn't
have yet is added (ALTER TABLE ... ADD COLUMN, nullable) in the same transaction as the batch
that first uses it, under a per-table advisory lock so concurrent consumers don't race. No
restart, no downtime. A column whose type changed incompatibly stops the sink loudly rather
than guessing.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

import orjson
import psycopg
from confluent_kafka import Consumer, KafkaError
from psycopg import sql

from .config import Settings
from .decode import Event, TableSchema, decode, utc_from_us

log = logging.getLogger(__name__)

META_COLS = ("_src_file", "_src_pos", "_src_row", "_src_commit_at", "_deleted")


class SchemaConflict(RuntimeError):
    pass


@dataclass
class BatchResult:
    received: int = 0
    deduped: int = 0  # superseded inside the batch by a newer event for the same key
    applied: int = 0
    stale: int = 0  # older than what the target already has (replays)


class Applier:
    """Applies decoded events to PostgreSQL inside the caller's open transaction."""

    def __init__(self, conn: psycopg.Connection, schema: str) -> None:
        self.conn = conn
        self.schema = schema
        self.columns: dict[str, dict[str, str]] = {}  # table -> {column: type}
        # IF NOT EXISTS isn't atomic: sinks starting together race on the catalog. Serialise the
        # bootstrap DDL (target schema + stats table) with one advisory lock.
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('cdcpipe-bootstrap'))")
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        conn.execute(
            "CREATE SCHEMA IF NOT EXISTS _cdc;"
            "CREATE TABLE IF NOT EXISTS _cdc.consumer_stats (run_id text, consumer_id text,"
            " pid int, started_at timestamptz, updated_at timestamptz, batches bigint,"
            " received bigint, deduped bigint, applied bigint, stale bigint, ops jsonb,"
            " lag_ms_hist jsonb, PRIMARY KEY (run_id, consumer_id, pid))"
        )
        conn.commit()

    # ------------------------------------------------------------------ DDL

    def _catalog(self, table: str) -> dict[str, str]:
        rows = self.conn.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a"
            " JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped",
            (self.schema, table),
        ).fetchall()
        return dict(rows)

    def ensure(self, table: str, ts: TableSchema) -> None:
        known = self.columns.get(table)
        if known is not None and all(
            f.name in known and _norm(known[f.name]) == f.pg_type for f in ts.fields
        ):
            return
        # Serialise DDL per table across consumers; re-read the catalog under the lock.
        self.conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{self.schema}.{table}",))
        known = self._catalog(table)
        ident = sql.Identifier(self.schema, table)
        if not known:
            cols: list[sql.Composable] = [
                sql.SQL("{} {}{}").format(
                    sql.Identifier(f.name),
                    sql.SQL(f.pg_type),
                    sql.SQL(" NOT NULL" if f.name in ts.key else ""),
                )
                for f in ts.fields
            ]
            cols += [
                sql.SQL("_src_file integer NOT NULL"),
                sql.SQL("_src_pos bigint NOT NULL"),
                sql.SQL("_src_row integer NOT NULL"),
                sql.SQL("_src_commit_at timestamptz NOT NULL"),
                sql.SQL("_deleted boolean NOT NULL DEFAULT false"),
                sql.SQL("_applied_at timestamptz NOT NULL DEFAULT clock_timestamp()"),
                sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(map(sql.Identifier, ts.key))),
            ]
            self.conn.execute(
                sql.SQL("CREATE TABLE {} ({})").format(ident, sql.SQL(", ").join(cols))
            )
            log.info("created %s.%s (%d columns)", self.schema, table, len(ts.fields))
        else:
            for f in ts.fields:
                have = known.get(f.name)
                if have is None:
                    self.conn.execute(
                        sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                            ident, sql.Identifier(f.name), sql.SQL(f.pg_type)
                        )
                    )
                    log.info("schema change: added %s.%s %s", table, f.name, f.pg_type)
                elif _base(have) != _base(f.pg_type):
                    raise SchemaConflict(
                        f"{table}.{f.name}: target is {have}, source is {f.pg_type}"
                    )
                elif _norm(have) != _norm(f.pg_type):
                    # Same type, different precision/scale (e.g. DECIMAL(10,2) -> (12,2)): follow
                    # the source. Narrowing would fail here on existing data, which is correct.
                    self.conn.execute(
                        sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE {}").format(
                            ident, sql.Identifier(f.name), sql.SQL(f.pg_type)
                        )
                    )
                    log.info("schema change: %s.%s %s -> %s", table, f.name, have, f.pg_type)
        self.columns[table] = self._catalog(table)

    # ------------------------------------------------------------------ DML

    def apply(self, events: list[Event]) -> BatchResult:
        res = BatchResult(received=len(events))
        newest: dict[tuple[str, tuple[Any, ...]], Event] = {}
        for e in events:
            k = (e.table, e.key)
            cur = newest.get(k)
            if cur is None or e.position > cur.position:
                newest[k] = e
        res.deduped = len(events) - len(newest)

        # Group by table and schema version: each group is one COPY + MERGE.
        groups: dict[tuple[str, TableSchema], list[Event]] = defaultdict(list)
        for e in newest.values():
            groups[(e.table, e.schema)].append(e)
        for i, ((table, ts), evs) in enumerate(sorted(groups.items(), key=lambda g: g[0][0])):
            self.ensure(table, ts)
            applied = self._merge(table, ts, evs, i)
            res.applied += applied
            res.stale += len(evs) - applied
            _hang("mid_apply")
        return res

    def _merge(self, table: str, ts: TableSchema, evs: list[Event], n: int) -> int:
        stage = sql.Identifier(f"_stage_{n}")
        cols = [*ts.names, *META_COLS]
        col_list = sql.SQL(", ").join(map(sql.Identifier, cols))
        self.conn.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                stage, sql.Identifier(self.schema, table)
            )
        )
        with (
            self.conn.cursor() as cur,
            cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(stage, col_list)) as copy,
        ):
            for e in evs:
                copy.write_row(
                    [
                        *(e.row[c] for c in ts.names),
                        *e.position,
                        utc_from_us(e.commit_us),
                        e.deleted,
                    ]
                )
        target = sql.Identifier(self.schema, table)
        # Write *every* target column, not just the event's: MySQL sends full row images, so a
        # column the event doesn't carry didn't exist in the source at that position and must be
        # NULL (the staging table holds NULL there). Updating only the event's columns once left a
        # stale value behind when a table was recreated without a column.
        all_cols = [c for c in self.columns[table] if c != "_applied_at"]
        updates = sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
            for c in all_cols
            if c not in ts.key
        )
        col_list = sql.SQL(", ").join(map(sql.Identifier, all_cols))
        cur = self.conn.execute(
            sql.SQL(
                "INSERT INTO {t} AS t ({cols}) SELECT {cols} FROM {s} "
                "ON CONFLICT ({pk}) DO UPDATE SET {u}, _applied_at = clock_timestamp() "
                "WHERE (t._src_file, t._src_pos, t._src_row) "
                "    < (EXCLUDED._src_file, EXCLUDED._src_pos, EXCLUDED._src_row)"
            ).format(
                t=target,
                cols=col_list,
                s=stage,
                pk=sql.SQL(", ").join(map(sql.Identifier, ts.key)),
                u=updates,
            )
        )
        return cur.rowcount


def _norm(pg_type: str) -> str:
    """format_type() spelling -> the spelling decode.py produces."""
    t = pg_type.replace(" without time zone", "")
    return t.replace("timestamp with time zone", "timestamptz").replace(" with time zone", "tz")


def _base(pg_type: str) -> str:
    return _norm(pg_type).split("(")[0]


def _hang(point: str) -> None:
    """Test hook. CDCPIPE_HANG=<point> freezes the process at that point of the first batch
    that applies rows, so a test can SIGKILL it there."""
    if os.environ.get("CDCPIPE_HANG") == point and not getattr(_hang, "done", False):
        _hang.done = True  # type: ignore[attr-defined]
        print(f"HANGING {point}", flush=True)
        time.sleep(3600)


# ---------------------------------------------------------------------- stats


@dataclass
class Stats:
    run_id: str
    consumer_id: str
    started_at: float = field(default_factory=time.time)
    batches: int = 0
    totals: BatchResult = field(default_factory=BatchResult)
    lag_ms: Counter[int] = field(default_factory=Counter)
    ops: Counter[str] = field(default_factory=Counter)

    def record(self, events: list[Event], res: BatchResult, committed_at: float) -> None:
        self.batches += 1
        for k in ("received", "deduped", "applied", "stale"):
            setattr(self.totals, k, getattr(self.totals, k) + getattr(res, k))
        now_us = committed_at * 1e6
        for e in events:
            self.ops[e.op] += 1
            if e.op != "r":  # snapshot rows have no meaningful commit time
                self.lag_ms[max(0, int((now_us - e.commit_us) / 1000))] += 1

    def flush(self, conn: psycopg.Connection) -> None:
        t = self.totals
        conn.execute(
            "INSERT INTO _cdc.consumer_stats VALUES"
            " (%s, %s, %s, to_timestamp(%s), now(), %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (run_id, consumer_id, pid) DO UPDATE SET updated_at = now(),"
            " batches = EXCLUDED.batches, received = EXCLUDED.received, deduped = EXCLUDED.deduped,"
            " applied = EXCLUDED.applied, stale = EXCLUDED.stale, ops = EXCLUDED.ops,"
            " lag_ms_hist = EXCLUDED.lag_ms_hist",
            (
                self.run_id,
                self.consumer_id,
                os.getpid(),
                self.started_at,
                self.batches,
                t.received,
                t.deduped,
                t.applied,
                t.stale,
                orjson.dumps(self.ops).decode(),
                orjson.dumps({str(k): v for k, v in self.lag_ms.items()}).decode(),
            ),
        )
        conn.commit()


# ---------------------------------------------------------------------- consumer


def consumer_config(s: Settings, group: str, instance: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": s.kafka,
        "group.id": group,
        # Static membership: a restarted consumer with the same instance id gets its partitions
        # back immediately instead of waiting out the session timeout and a rebalance.
        "group.instance.id": instance,
        "session.timeout.ms": 10000,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "fetch.wait.max.ms": 20,  # librdkafka waits 500 ms by default: most of the lag budget
        # A regex subscription only sees a new table's topic on the next metadata refresh.
        "topic.metadata.refresh.interval.ms": 1000,
        # Incremental rebalancing: a new topic or member moves only the affected partitions
        # instead of revoking everyone's partitions (stop-the-world with the default assignors).
        "partition.assignment.strategy": "cooperative-sticky",
    }


def run(
    s: Settings,
    group: str = "cdc-sink",
    instance: str = "sink-1",
    run_id: str | None = None,
    batch_max: int = 2000,
    idle_exit_s: float | None = None,
) -> Stats:
    stop = False

    def _stop(*_: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    pg = s.pg(app=instance)
    applier = Applier(pg, s.target_schema)
    stats = Stats(run_id or uuid.uuid4().hex[:8], instance)
    c = Consumer(consumer_config(s, group, instance))
    c.subscribe([s.topic_regex])
    last_flush = last_msg = time.time()
    log.info("%s consuming %s as group %s", instance, s.topic_regex, group)
    try:
        while not stop:
            msgs = c.consume(num_messages=batch_max, timeout=0.05)
            now = time.time()
            if now - last_flush > 2:
                stats.flush(pg)
                last_flush = now
            if not msgs:
                if idle_exit_s is not None and now - last_msg > idle_exit_s:
                    break
                continue
            events: list[Event] = []
            real = 0  # messages that carry an offset to commit (tombstones included)
            for m in msgs:
                err = m.error()
                if err is not None:
                    # A regex subscription that matches no topic yet (connector still starting, or
                    # a table with no changes so far) is normal: keep polling.
                    if err.code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        continue
                    raise RuntimeError(f"kafka error: {err}")
                real += 1
                e = decode(str(m.topic()), m.key(), m.value(), m.partition() or 0, m.offset() or 0)
                if e is not None:
                    events.append(e)
            if not real:
                continue
            last_msg = now
            res = applier.apply(events) if events else BatchResult()
            pg.commit()
            committed = time.time()
            stats.record(events, res, committed)
            if res.applied:
                _hang("after_pg_commit")
            c.commit(asynchronous=False)
    except BaseException:
        pg.rollback()
        raise
    finally:
        stats.flush(pg)
        c.close()
        pg.close()
    return stats


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    run(Settings.from_env(), instance=sys.argv[1] if len(sys.argv) > 1 else "sink-1")
