"""Order-processing workload against the MySQL source.

Each operation is one short transaction, like an application would issue:

  op               rows changed                                          weight
  new_customer     1 insert                                                  5
  update_customer  1 update (address; loyalty tier after the schema change)  10
  place_order      1 order + k items inserted, k product stocks updated     30
  advance_order    1 update (placed -> paid -> shipped -> delivered)         34
  cancel_order     1 update + k stock updates (restock)                      5
  reprice_product  1 update                                                  8
  purge_order      k item deletes + 1 order delete                           8

Every row change becomes exactly one Debezium event, so the generator counts *row changes* (from
cursor.rowcount) and stops at the requested total. Writers run as separate processes (the GIL
would cap a threaded generator well below the rates the pipeline handles); a shared token bucket
holds the aggregate rate.

`schema_change_at` adds a column to `customers` mid-run (MySQL 8 does it INSTANT, no table
copy); writers notice through a shared flag and start filling it in.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import random
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pymysql

from .config import Settings

log = logging.getLogger(__name__)

DDL = [
    """CREATE TABLE IF NOT EXISTS customers (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(120) NOT NULL,
        full_name VARCHAR(80) NOT NULL,
        city VARCHAR(60) NOT NULL,
        state CHAR(2) NOT NULL,
        birth_date DATE NULL,
        marketing_opt_in TINYINT(1) NOT NULL DEFAULT 0,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6))""",
    """CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sku VARCHAR(20) NOT NULL UNIQUE,
        name VARCHAR(80) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        stock INT NOT NULL,
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6))""",
    """CREATE TABLE IF NOT EXISTS orders (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        status VARCHAR(12) NOT NULL,
        total DECIMAL(12,2) NOT NULL,
        placed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        KEY ix_orders_customer (customer_id))""",
    """CREATE TABLE IF NOT EXISTS order_items (
        order_id BIGINT NOT NULL,
        line_no SMALLINT NOT NULL,
        product_id INT NOT NULL,
        qty SMALLINT NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (order_id, line_no))""",
    """CREATE TABLE IF NOT EXISTS cdc_markers (
        id VARCHAR(36) PRIMARY KEY,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))""",
]
SCHEMA_CHANGE = "ALTER TABLE customers ADD COLUMN loyalty_tier VARCHAR(10) NULL, ALGORITHM=INSTANT"

STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "PE"]
CITIES = ["Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Porto Alegre", "Curitiba", "Recife"]
NEXT_STATUS = {"placed": "paid", "paid": "shipped", "shipped": "delivered"}
OPS = [
    ("new_customer", 5),
    ("update_customer", 10),
    ("place_order", 30),
    ("advance_order", 34),
    ("cancel_order", 5),
    ("reprice_product", 8),
    ("purge_order", 8),
]


def create_schema(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cur:
        for d in DDL:
            cur.execute(d)
    conn.commit()


def reset(s: Settings) -> None:
    root = s.mysql(db="")
    with root.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS `{s.mysql_db}`")
        cur.execute(f"CREATE DATABASE `{s.mysql_db}`")
    root.commit()
    root.close()
    conn = s.mysql()
    create_schema(conn)
    conn.close()


def seed(s: Settings, customers: int = 20_000, products: int = 2_000, seed: int = 1) -> int:
    """Initial data, written before the connector starts, so it arrives as snapshot events."""
    rnd = random.Random(seed)
    conn = s.mysql()
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO customers (email, full_name, city, state, birth_date, marketing_opt_in)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (
                    f"user{i}@example.com",
                    f"Customer {i}",
                    rnd.choice(CITIES),
                    rnd.choice(STATES),
                    None
                    if rnd.random() < 0.3
                    else f"19{rnd.randint(50, 99)}-0{rnd.randint(1, 9)}-1{rnd.randint(0, 9)}",
                    rnd.random() < 0.4,
                )
                for i in range(customers)
            ],
        )
        cur.executemany(
            "INSERT INTO products (sku, name, price, stock) VALUES (%s, %s, %s, %s)",
            [
                (
                    f"SKU-{i:06d}",
                    f"Product {i}",
                    Decimal(rnd.randint(199, 99999)) / 100,
                    rnd.randint(50, 5000),
                )
                for i in range(products)
            ],
        )
        # A few orders too, so every table already has a topic when streaming starts.
        n_orders = max(1, customers // 20)
        cur.executemany(
            "INSERT INTO orders (customer_id, status, total) VALUES (%s, 'delivered', %s)",
            [
                (rnd.randint(1, customers), Decimal(rnd.randint(500, 50000)) / 100)
                for _ in range(n_orders)
            ],
        )
        cur.execute("SELECT id FROM orders")
        ids = [r[0] for r in cur.fetchall()]
        cur.executemany(
            "INSERT INTO order_items (order_id, line_no, product_id, qty, unit_price)"
            " VALUES (%s, 1, %s, 1, 9.99)",
            [(i, rnd.randint(1, products)) for i in ids],
        )
        cur.execute("INSERT INTO cdc_markers (id) VALUES ('seed')")
    conn.commit()
    conn.close()
    return customers + products + 2 * n_orders + 1


@dataclass
class Shared:
    """State shared by the writer processes."""

    events: Any  # mp.Value('q'): row changes so far
    tokens: Any  # mp.Value('d'): token bucket
    last: Any  # mp.Value('d'): last refill time
    schema_v2: Any  # mp.Value('b')
    max_customer: Any
    max_order: Any
    lock: Any


class Writer:
    def __init__(self, s: Settings, sh: Shared, rate: float, seed: int, n_products: int) -> None:
        self.conn = s.mysql()
        self.conn.autocommit(False)
        self.cur = self.conn.cursor()
        self.sh = sh
        self.rate = rate
        self.rnd = random.Random(seed)
        self.n_products = n_products
        self.ops, self.weights = zip(*OPS, strict=True)

    def _take(self, n: int) -> None:
        """Token bucket shared across processes: blocks until n events are allowed."""
        while True:
            with self.sh.lock:
                now = time.monotonic()
                self.sh.tokens.value = min(
                    self.rate * 0.25, self.sh.tokens.value + (now - self.sh.last.value) * self.rate
                )
                self.sh.last.value = now
                if self.sh.tokens.value >= n:
                    self.sh.tokens.value -= n
                    return
                wait = (n - self.sh.tokens.value) / self.rate
            time.sleep(min(wait, 0.01))

    def _x(self, q: str, args: tuple[Any, ...] = ()) -> int:
        self.cur.execute(q, args)
        return int(self.cur.rowcount)

    def step(self) -> int:
        op = self.rnd.choices(self.ops, self.weights)[0]
        n = getattr(self, op)()
        self.conn.commit()
        return n

    # ---- operations; each returns the number of rows changed

    def new_customer(self) -> int:
        i = self.rnd.randrange(10**9)
        n = self._x(
            "INSERT INTO customers (email, full_name, city, state) VALUES (%s, %s, %s, %s)",
            (f"new{i}@example.com", f"New {i}", self.rnd.choice(CITIES), self.rnd.choice(STATES)),
        )
        with self.sh.lock:
            self.sh.max_customer.value = max(self.sh.max_customer.value, self.cur.lastrowid or 0)
        return n

    def update_customer(self) -> int:
        cid = self.rnd.randint(1, self.sh.max_customer.value)
        if self.sh.schema_v2.value:
            return self._x(
                "UPDATE customers SET city = %s, state = %s, loyalty_tier = %s WHERE id = %s",
                (
                    self.rnd.choice(CITIES),
                    self.rnd.choice(STATES),
                    self.rnd.choice(["bronze", "silver", "gold"]),
                    cid,
                ),
            )
        return self._x(
            "UPDATE customers SET city = %s, state = %s WHERE id = %s",
            (self.rnd.choice(CITIES), self.rnd.choice(STATES), cid),
        )

    def place_order(self) -> int:
        k = self.rnd.randint(1, 4)
        # Sorted, so concurrent orders lock product rows in the same order (no deadlock cycles).
        prods = sorted(self.rnd.sample(range(1, self.n_products + 1), k))
        self.cur.execute(
            f"SELECT id, price FROM products WHERE id IN ({','.join(['%s'] * k)})", prods
        )
        prices = dict(self.cur.fetchall())
        if not prices:
            return 0
        lines = [(p, self.rnd.randint(1, 3), prices[p]) for p in prods if p in prices]
        total = sum(q * pr for _, q, pr in lines)
        n = self._x(
            "INSERT INTO orders (customer_id, status, total) VALUES (%s, 'placed', %s)",
            (self.rnd.randint(1, self.sh.max_customer.value), total),
        )
        oid = self.cur.lastrowid
        self.cur.executemany(
            "INSERT INTO order_items (order_id, line_no, product_id, qty, unit_price)"
            " VALUES (%s, %s, %s, %s, %s)",
            [(oid, i + 1, p, q, pr) for i, (p, q, pr) in enumerate(lines)],
        )
        n += self.cur.rowcount
        for p, q, _ in lines:
            n += self._x("UPDATE products SET stock = stock - %s WHERE id = %s", (q, p))
        with self.sh.lock:
            self.sh.max_order.value = max(self.sh.max_order.value, oid or 0)
        return n

    def _recent_order(self) -> int:
        hi = self.sh.max_order.value
        return self.rnd.randint(max(1, hi - 5000), hi) if hi else 0

    def advance_order(self) -> int:
        oid = self._recent_order()
        self.cur.execute("SELECT status FROM orders WHERE id = %s FOR UPDATE", (oid,))
        row = self.cur.fetchone()
        if not row or row[0] not in NEXT_STATUS:
            return 0
        return self._x("UPDATE orders SET status = %s WHERE id = %s", (NEXT_STATUS[row[0]], oid))

    def cancel_order(self) -> int:
        oid = self._recent_order()
        n = self._x(
            "UPDATE orders SET status = 'canceled' WHERE id = %s AND status IN ('placed', 'paid')",
            (oid,),
        )
        if n:
            self.cur.execute(
                "SELECT product_id, qty FROM order_items WHERE order_id = %s ORDER BY product_id",
                (oid,),
            )
            for p, q in self.cur.fetchall():
                n += self._x("UPDATE products SET stock = stock + %s WHERE id = %s", (q, p))
        return n

    def reprice_product(self) -> int:
        return self._x(
            "UPDATE products SET price = %s WHERE id = %s",
            (Decimal(self.rnd.randint(199, 99999)) / 100, self.rnd.randint(1, self.n_products)),
        )

    def purge_order(self) -> int:
        hi = self.sh.max_order.value
        if hi < 100:
            return 0
        oid = self.rnd.randint(1, int(hi * 0.7))  # old orders only
        self.cur.execute("SELECT status FROM orders WHERE id = %s FOR UPDATE", (oid,))
        row = self.cur.fetchone()
        if not row or row[0] not in ("delivered", "canceled"):
            return 0
        n = self._x("DELETE FROM order_items WHERE order_id = %s", (oid,))
        return n + self._x("DELETE FROM orders WHERE id = %s", (oid,))


def _writer_main(
    s: Settings, sh: Shared, rate: float, seed: int, n_products: int, target: int
) -> None:
    w = Writer(s, sh, rate, seed, n_products)
    while sh.events.value < target:
        w._take(3)  # average events per op is ~2.9; true count corrected below
        for attempt in range(3):
            try:
                n = w.step()
                break
            except pymysql.err.OperationalError as e:  # deadlock / lock wait: retry the op
                w.conn.rollback()
                if e.args[0] not in (1213, 1205) or attempt == 2:
                    raise
        with sh.lock:
            sh.events.value += n


def run(
    s: Settings,
    events: int,
    rate: float,
    writers: int = 4,
    n_products: int = 2_000,
    schema_change_at: int | None = None,
    progress: Any = None,
) -> dict[str, Any]:
    """Generates `events` row changes at about `rate` per second. Returns timing."""
    conn = s.mysql()
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM customers")
        max_c = cur.fetchone()[0]  # type: ignore[index]
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM orders")
        max_o = cur.fetchone()[0]  # type: ignore[index]
        cur.execute(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = %s"
            " AND table_name = 'customers' AND column_name = 'loyalty_tier'",
            (s.mysql_db,),
        )
        v2 = cur.fetchone()[0]  # type: ignore[index]
    conn.commit()
    ctx = mp.get_context("spawn")
    sh = Shared(
        ctx.Value("q", 0),
        ctx.Value("d", 0.0),
        ctx.Value("d", time.monotonic()),
        ctx.Value("b", int(bool(v2))),
        ctx.Value("q", max_c),
        ctx.Value("q", max_o),
        ctx.Lock(),
    )
    procs = [
        ctx.Process(
            target=_writer_main, args=(s, sh, rate, 1000 + i, n_products, events), daemon=True
        )
        for i in range(writers)
    ]
    t0 = time.time()
    for p in procs:
        p.start()
    changed_at = None
    while any(p.is_alive() for p in procs):
        if (
            schema_change_at is not None
            and changed_at is None
            and sh.events.value >= schema_change_at
        ):
            with conn.cursor() as cur:
                cur.execute(SCHEMA_CHANGE)
            conn.commit()
            sh.schema_v2.value = 1
            changed_at = time.time() - t0
            log.info("schema change applied at %d events (%.1f s)", sh.events.value, changed_at)
        if progress:
            progress(sh.events.value, time.time() - t0)
        time.sleep(0.2)
    for p in procs:
        p.join()
        if p.exitcode:
            raise RuntimeError(f"writer exited with {p.exitcode}")
    conn.close()
    elapsed = time.time() - t0
    return {
        "events": sh.events.value,
        "seconds": round(elapsed, 2),
        "rate": round(sh.events.value / elapsed, 1),
        "schema_change_at_s": changed_at,
    }
