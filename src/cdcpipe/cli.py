"""Command line: cdcpipe <command>

reset                         drop and recreate the source database and the target schema
seed [--customers N]          initial rows (become snapshot events when the connector starts)
connector create|delete|status
sink [--instance ID]          run a sink consumer (start several for parallelism)
load --events N --rate R      generate N row changes at ~R/s [--schema-change-at K]
verify                        wait for catch-up, then compare every table; exit 1 on mismatch
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from psycopg import sql

from . import connector, sink, verify, workload
from .config import Settings


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(
        prog="cdcpipe", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    rs = sub.add_parser("reset")
    rs.add_argument("--group", action="append", default=[], help="consumer group(s) to delete")
    sd = sub.add_parser("seed")
    sd.add_argument("--customers", type=int, default=20_000)
    sd.add_argument("--products", type=int, default=2_000)
    cn = sub.add_parser("connector")
    cn.add_argument("action", choices=["create", "delete", "status"])
    cn.add_argument("--server-id", type=int, default=5501)
    sk = sub.add_parser("sink")
    sk.add_argument("--instance", default="sink-1")
    sk.add_argument("--group", default="cdc-sink")
    sk.add_argument("--run-id")
    sk.add_argument("--batch", type=int, default=2000)
    sk.add_argument("--idle-exit", type=float, help="exit after this many idle seconds")
    ld = sub.add_parser("load")
    ld.add_argument("--events", type=int, required=True)
    ld.add_argument("--rate", type=float, default=2000)
    ld.add_argument("--writers", type=int, default=4)
    ld.add_argument("--products", type=int, default=2_000)
    ld.add_argument("--schema-change-at", type=int)
    vf = sub.add_parser("verify")
    vf.add_argument("--group", default="cdc-sink")
    vf.add_argument("--timeout", type=float, default=300)

    a = p.parse_args(argv)
    s = Settings.from_env()

    if a.cmd == "reset":
        connector.delete(s)
        gone = connector.delete_topics_and_groups(s, a.group or ["cdc-sink"])
        workload.reset(s)
        with s.pg(autocommit=True) as pg:
            pg.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(s.target_schema))
            )
        print(
            f"reset source database {s.mysql_db}, target schema {s.target_schema},"
            f" {len(gone)} topics"
        )
    elif a.cmd == "seed":
        print(f"seeded {workload.seed(s, a.customers, a.products):,} rows")
    elif a.cmd == "connector":
        if a.action == "create":
            print(json.dumps(connector.create(s, a.server_id), indent=1))
        elif a.action == "delete":
            connector.delete(s)
        else:
            print(json.dumps(connector.status(s), indent=1))
    elif a.cmd == "sink":
        st = sink.run(
            s,
            group=a.group,
            instance=a.instance,
            run_id=a.run_id,
            batch_max=a.batch,
            idle_exit_s=a.idle_exit,
        )
        print(f"{a.instance}: {st.batches} batches, {st.totals}")
    elif a.cmd == "load":
        print(
            json.dumps(workload.run(s, a.events, a.rate, a.writers, a.products, a.schema_change_at))
        )
    elif a.cmd == "verify":
        waited = verify.wait_caught_up(s, a.group, a.timeout)
        reports = verify.compare(s)
        for r in reports:
            print(
                f"{'ok  ' if r.ok else 'FAIL'} {r.table:14s} source {r.source_rows:>8,}"
                f"  target {r.target_rows:>8,}  missing {r.missing}  extra {r.extra}"
                f"  mismatched {r.mismatched}  sha256 {r.source_sha256[:12]}"
                f"{'' if r.ok else ' != ' + r.target_sha256[:12]} {' '.join(r.examples)}"
            )
        print(f"caught up {waited:.1f}s after the marker")
        return 0 if all(r.ok for r in reports) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
