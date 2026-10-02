"""End to end through the real stack: MySQL binlog -> Debezium -> Kafka -> sink -> PostgreSQL.

Each test gets its own source database (shop_test), topic prefix, connector, consumer group and
target schema, so tests are isolated from each other and from the demo data.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import replace
from typing import IO

import httpx
import pytest

from cdcpipe import connector, verify, workload
from cdcpipe.config import Settings

pytestmark = pytest.mark.integration


@pytest.fixture
def stack() -> Iterator[tuple[Settings, str, dict[str, str]]]:
    base = Settings.from_env()
    try:
        httpx.get(base.connect_url + "/connectors", timeout=3).raise_for_status()
    except Exception as e:  # pragma: no cover
        pytest.skip(f"stack not running ({e}); run `make up`")
    tag = uuid.uuid4().hex[:6]
    s = replace(base, mysql_db="shop_test", topic_prefix=f"t{tag}", target_schema=f"t_{tag}")
    group = f"g{tag}"
    env = {
        **os.environ,
        "MYSQL_DATABASE": s.mysql_db,
        "TOPIC_PREFIX": s.topic_prefix,
        "TARGET_SCHEMA": s.target_schema,
    }
    workload.reset(s)
    workload.seed(s, customers=500, products=100)
    connector.create(s, server_id=7000 + int(tag, 16) % 1000)
    yield s, group, env
    connector.delete(s)
    connector.delete_topics_and_groups(s, [group])


def start_sink(
    env: dict[str, str], group: str, instance: str = "sink-1", hang: str | None = None
) -> subprocess.Popen[str]:
    e = {**env, **({"CDCPIPE_HANG": hang} if hang else {})}
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "cdcpipe",
            "sink",
            "--instance",
            instance,
            "--group",
            group,
            "--batch",
            "500",
        ],
        env=e,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_for(stream: IO[str] | None, marker: str, timeout: float = 60) -> None:
    assert stream is not None
    deadline = time.time() + timeout
    for line in stream:
        if marker in line:
            return
        if time.time() > deadline:
            break
    raise AssertionError(f"never saw {marker!r}")


def stop(*procs: subprocess.Popen[str]) -> None:
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    for p in procs:
        p.wait(timeout=30)


def assert_replica_matches(s: Settings, group: str) -> None:
    verify.wait_caught_up(s, group, timeout_s=120)
    reports = verify.compare(s)
    bad = [r for r in reports if not r.ok]
    assert not bad, [(r.table, r.missing, r.extra, r.mismatched, r.examples) for r in bad]
    assert {r.table for r in reports} >= {"customers", "orders", "order_items", "products"}


def test_snapshot_stream_schema_change_and_two_sinks(
    stack: tuple[Settings, str, dict[str, str]],
) -> None:
    s, group, env = stack
    sinks = [start_sink(env, group, f"sink-{i}") for i in (1, 2)]
    try:
        out = workload.run(
            s, events=6000, rate=3000, writers=3, n_products=100, schema_change_at=3000
        )
        assert out["schema_change_at_s"] is not None
        assert_replica_matches(s, group)
        with s.pg() as pg:
            cols = pg.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = %s AND table_name = 'customers'",
                (s.target_schema,),
            ).fetchall()
            assert ("loyalty_tier",) in cols
            deleted = pg.execute(
                f'SELECT count(*) FROM "{s.target_schema}".orders WHERE _deleted'
            ).fetchone()
            assert deleted and deleted[0] > 0, "the workload purges orders"
    finally:
        stop(*sinks)


def test_kill_after_postgres_commit_before_kafka_commit(
    stack: tuple[Settings, str, dict[str, str]],
) -> None:
    """The worst spot for at-least-once: rows are in PostgreSQL, offsets aren't in Kafka. The
    restarted sink replays the batch; the position guard turns it into no-ops."""
    s, group, env = stack
    victim = start_sink(env, group, hang="after_pg_commit")
    wait_for(victim.stdout, "HANGING after_pg_commit")
    victim.kill()
    victim.wait()
    survivor = start_sink(env, group)
    try:
        workload.run(s, events=2000, rate=2000, writers=2, n_products=100)
        assert_replica_matches(s, group)
    finally:
        stop(survivor)
    with s.pg() as pg:
        stale = pg.execute(
            "SELECT sum(stale) FROM _cdc.consumer_stats WHERE consumer_id = 'sink-1'"
            " AND run_id IN (SELECT run_id FROM _cdc.consumer_stats"
            " WHERE updated_at > now() - interval '5 minutes')"
        ).fetchone()
    assert stale and stale[0] > 0, "the replayed batch should have been skipped as stale"


def test_kill_mid_apply_rolls_back(stack: tuple[Settings, str, dict[str, str]]) -> None:
    """Killed between two tables of one batch, inside the PostgreSQL transaction."""
    s, group, env = stack
    workload.run(s, events=1500, rate=3000, writers=2, n_products=100)
    victim = start_sink(env, group, hang="mid_apply")
    wait_for(victim.stdout, "HANGING mid_apply")
    with s.pg() as pg:
        state = pg.execute(
            "SELECT state FROM pg_stat_activity WHERE application_name = 'sink-1'"
        ).fetchall()
    assert ("idle in transaction",) in state  # really mid-transaction
    victim.kill()
    victim.wait()
    survivor = start_sink(env, group)
    try:
        assert_replica_matches(s, group)
    finally:
        stop(survivor)


def test_cli_round_trip(
    stack: tuple[Settings, str, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cdcpipe import cli

    _, group, env = stack
    for k in ("MYSQL_DATABASE", "TOPIC_PREFIX", "TARGET_SCHEMA"):
        monkeypatch.setenv(k, env[k])
    sink = start_sink(env, group)
    try:
        assert (
            cli.main(
                [
                    "load",
                    "--events",
                    "1500",
                    "--rate",
                    "3000",
                    "--writers",
                    "2",
                    "--products",
                    "100",
                ]
            )
            == 0
        )
        assert cli.main(["connector", "status"]) == 0
        assert cli.main(["verify", "--group", group, "--timeout", "120"]) == 0
    finally:
        stop(sink)
    out = capsys.readouterr().out
    assert '"RUNNING"' in out and "ok   orders" in out
    assert cli.main(["reset", "--group", group]) == 0
    assert cli.main(["seed", "--customers", "50", "--products", "10"]) == 0
    assert cli.main(["connector", "create", "--server-id", "7999"]) == 0
    assert cli.main(["connector", "delete"]) == 0
