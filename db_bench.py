#!/usr/bin/env python3
"""
Database load generator for Linux (PostgreSQL / MySQL).
Usage: see --help
"""

from __future__ import annotations

import argparse
import random
import re
import sys
import statistics
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# --- drivers (import lazily after URL check) ---

# Cap latency samples per reporting window to bound memory under very high QPS.
_MAX_INTERVAL_LAT_SAMPLES = 100_000

# Avoid hanging forever when the server is down; reconnect uses the same timeouts.
_CONNECT_TIMEOUT_SEC = 10
_MYSQL_SOCK_TIMEOUT_SEC = 30

# SQL identifiers: table stem and generated names (no quoting).
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_IDENT_LEN = 60

_MYSQL_FILL_CHUNK = 1000

# Defaults for CLI and config file (YAML). Keys use snake_case; YAML may use hyphens (normalized).
_PROGRAM_DEFAULTS: Dict[str, object] = {
    "url": None,
    "workers": 8,
    "duration": 30.0,
    "mode": "oltp",
    "select_ratio": 0.5,
    "insert_ratio": 0.5,
    "update_ratio": 0.0,
    "delete_ratio": 0.0,
    "txn_mode": "single",
    "txn_statements": 7,
    "txn_select_ratio": 5.0,
    "txn_insert_ratio": 1.0,
    "txn_update_ratio": 1.0,
    "txn_delete_ratio": 1.0,
    "table": "db_bench_load",
    "tables": 1,
    "table_size": 0,
    "prepare": False,
    "warmup": 0.0,
    "report_interval": 0.0,
}

_CONFIG_ALLOWED_KEYS = frozenset(_PROGRAM_DEFAULTS.keys())


def extract_config_path(argv: List[str]) -> Tuple[Optional[str], List[str]]:
    """Remove -c/--config PATH from argv; return (path, remaining argv for main parser)."""
    out: List[str] = []
    i = 0
    cfg: Optional[str] = None
    while i < len(argv):
        a = argv[i]
        if a in ("-c", "--config"):
            if i + 1 >= len(argv):
                raise SystemExit(f"{a} requires a file path")
            cfg = argv[i + 1]
            i += 2
            continue
        if a.startswith("--config="):
            cfg = a.split("=", 1)[1]
            if not cfg:
                raise SystemExit("--config= requires a non-empty path")
            i += 1
            continue
        out.append(a)
        i += 1
    return cfg, out


def _coerce_config_value(key: str, val: object) -> object:
    if key == "prepare":
        if isinstance(val, str):
            return val.strip().lower() in ("1", "true", "yes", "on")
        return bool(val)
    return val


def load_config_file(path: str) -> dict:
    lp = path.lower()
    if not lp.endswith((".yaml", ".yml")):
        raise SystemExit(f"Config file must end with .yaml or .yml: {path!r}")
    try:
        import yaml
    except ImportError as e:
        raise SystemExit("PyYAML is required for --config: pip install pyyaml") from e
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise SystemExit("Config root must be a YAML mapping (dictionary)")
    return data


def normalize_config_mapping(raw: dict) -> dict:
    out: Dict[str, object] = {}
    for k, v in raw.items():
        nk = str(k).replace("-", "_")
        if nk not in _CONFIG_ALLOWED_KEYS:
            print(f"Warning: unknown config key ignored: {k!r}", file=sys.stderr)
            continue
        out[nk] = _coerce_config_value(nk, v)
    return out


def merge_config_with_program_defaults(file_cfg: dict) -> dict:
    d = dict(_PROGRAM_DEFAULTS)
    for k, v in file_cfg.items():
        d[k] = v
    return d


def build_argument_parser(defaults: Dict[str, object]) -> argparse.ArgumentParser:
    d = defaults
    p = argparse.ArgumentParser(
        description="Simple DB load test (PostgreSQL / MySQL).",
        epilog="Required: database URL via --url or config key 'url'. "
        "Use -c/--config FILE.yaml to load options; CLI overrides file. "
        "See README for all keys and defaults.",
    )
    p.add_argument(
        "--url",
        default=d["url"],
        help="Database URL (required if not set in config file)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=d["workers"],
        help="Concurrent client threads (default: %(default)s)",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=d["duration"],
        help="Benchmark duration in seconds (default: %(default)s)",
    )
    p.add_argument(
        "--mode",
        choices=["oltp", "read", "write"],
        default=d["mode"],
        help="oltp: use CRUD ratios below; read: 100%% SELECT; write: 100%% INSERT (default: %(default)s)",
    )
    p.add_argument(
        "--select-ratio",
        type=float,
        default=d["select_ratio"],
        metavar="W",
        help="SELECT weight for --txn-mode single / outer mix (default: %(default)s)",
    )
    p.add_argument(
        "--insert-ratio",
        type=float,
        default=d["insert_ratio"],
        metavar="W",
        help="INSERT weight (default: %(default)s)",
    )
    p.add_argument(
        "--update-ratio",
        type=float,
        default=d["update_ratio"],
        metavar="W",
        help="UPDATE weight (default: %(default)s)",
    )
    p.add_argument(
        "--delete-ratio",
        type=float,
        default=d["delete_ratio"],
        metavar="W",
        help="DELETE weight (default: %(default)s)",
    )
    p.add_argument(
        "--txn-mode",
        choices=["single", "multi"],
        default=d["txn_mode"],
        help="single or multi-statement transactions (default: %(default)s)",
    )
    p.add_argument(
        "--txn-statements",
        type=int,
        default=d["txn_statements"],
        metavar="N",
        help="Statements per transaction when --txn-mode multi (default: %(default)s)",
    )
    p.add_argument(
        "--txn-select-ratio",
        type=float,
        default=d["txn_select_ratio"],
        metavar="W",
        help="Txn-internal SELECT weight (default: %(default)s)",
    )
    p.add_argument(
        "--txn-insert-ratio",
        type=float,
        default=d["txn_insert_ratio"],
        metavar="W",
        help="Txn-internal INSERT weight (default: %(default)s)",
    )
    p.add_argument(
        "--txn-update-ratio",
        type=float,
        default=d["txn_update_ratio"],
        metavar="W",
        help="Txn-internal UPDATE weight (default: %(default)s)",
    )
    p.add_argument(
        "--txn-delete-ratio",
        type=float,
        default=d["txn_delete_ratio"],
        metavar="W",
        help="Txn-internal DELETE weight (default: %(default)s)",
    )
    p.add_argument(
        "--table",
        default=d["table"],
        help="Table name or stem (default: %(default)s)",
    )
    p.add_argument(
        "--tables",
        type=int,
        default=d["tables"],
        help="Number of tables (default: %(default)s)",
    )
    p.add_argument(
        "--table-size",
        type=int,
        default=d["table_size"],
        metavar="N",
        help="Rows per table on --prepare if empty (default: %(default)s)",
    )
    p.add_argument(
        "--prepare",
        action="store_true",
        default=d["prepare"],
        help="Create benchmark tables/indexes if missing",
    )
    p.add_argument(
        "--no-prepare",
        action="store_true",
        help="Force prepare off (overrides config and --prepare)",
    )
    p.add_argument(
        "--warmup",
        type=float,
        default=d["warmup"],
        help="Warmup seconds before measured run (default: %(default)s)",
    )
    p.add_argument(
        "--report-interval",
        type=float,
        default=d["report_interval"],
        metavar="SEC",
        help="Print stats every SEC seconds; 0 disables (default: %(default)s)",
    )
    return p


class LiveStats:
    """Thread-safe counters for periodic reporting (measurement phase only)."""

    __slots__ = ("_lock", "ops", "errors", "interval_latencies")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.ops = 0
        self.errors = 0
        self.interval_latencies: List[float] = []

    def record_op(self, ms: float) -> None:
        with self._lock:
            self.ops += 1
            if len(self.interval_latencies) < _MAX_INTERVAL_LAT_SAMPLES:
                self.interval_latencies.append(ms)

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1

    def snapshot_interval(self) -> Tuple[int, int, List[float]]:
        """Return cumulative ops/errors and latencies since last snapshot; clear latency buffer."""
        with self._lock:
            o, e = self.ops, self.errors
            lats = self.interval_latencies
            self.interval_latencies = []
            return o, e, lats


@dataclass
class BenchConfig:
    url: str
    workers: int
    duration_sec: float
    select_ratio: float
    insert_ratio: float
    update_ratio: float
    delete_ratio: float
    txn_mode: str
    txn_statements: int
    txn_select_ratio: float
    txn_insert_ratio: float
    txn_update_ratio: float
    txn_delete_ratio: float
    table: str
    tables: int
    table_size: int
    prepare: bool
    warmup_sec: float
    report_interval_sec: float


@dataclass
class WorkerResult:
    ops: int = 0
    errors: int = 0
    latencies_ms: List[float] = field(default_factory=list)


def parse_db_url(url: str) -> Tuple[str, dict]:
    """Return (scheme, connection kwargs)."""
    parsed = urllib.parse.urlparse(url)
    scheme = (parsed.scheme or "").lower().replace("+asyncpg", "").replace("+psycopg2", "")
    if scheme not in ("postgresql", "postgres", "mysql"):
        raise SystemExit(
            f"Unsupported URL scheme {parsed.scheme!r}. Use postgresql:// or mysql://"
        )
    user = urllib.parse.unquote(parsed.username or "")
    password = urllib.parse.unquote(parsed.password or "") if parsed.password else ""
    host = parsed.hostname or "localhost"
    port = parsed.port
    db = (parsed.path or "/").lstrip("/") or "postgres"
    return scheme, {
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "database": db,
    }


def connect_postgres(kwargs: dict):
    import psycopg2

    port = kwargs["port"] or 5432
    return psycopg2.connect(
        host=kwargs["host"],
        port=port,
        user=kwargs["user"],
        password=kwargs["password"],
        dbname=kwargs["database"],
        connect_timeout=_CONNECT_TIMEOUT_SEC,
    )


def connect_mysql(kwargs: dict):
    import pymysql

    port = kwargs["port"] or 3306
    return pymysql.connect(
        host=kwargs["host"],
        port=port,
        user=kwargs["user"],
        password=kwargs["password"],
        database=kwargs["database"],
        autocommit=True,
        connect_timeout=_CONNECT_TIMEOUT_SEC,
        read_timeout=_MYSQL_SOCK_TIMEOUT_SEC,
        write_timeout=_MYSQL_SOCK_TIMEOUT_SEC,
    )


def _open_connection(scheme: str, kwargs: dict):
    if scheme in ("postgresql", "postgres"):
        return connect_postgres(kwargs)
    return connect_mysql(kwargs)


def _safe_close_conn(conn) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def _is_connection_level_error(exc: BaseException, scheme: str) -> bool:
    """True if the client session is likely unusable (restart, failover, network drop)."""
    if isinstance(exc, (TimeoutError, ConnectionError, BrokenPipeError)):
        return True
    if scheme in ("postgresql", "postgres"):
        import psycopg2

        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))
    import pymysql.err

    return isinstance(exc, (pymysql.err.OperationalError, pymysql.err.InterfaceError))


def _check_identifier(name: str, what: str) -> None:
    if len(name) > _MAX_IDENT_LEN:
        raise SystemExit(f"{what} is too long (max {_MAX_IDENT_LEN} characters): {name!r}")
    if not _IDENT_RE.match(name):
        raise SystemExit(
            f"Invalid {what} {name!r}: use ASCII letters, digits, underscore; "
            "must start with a letter or underscore"
        )


def normalize_crud_ratios(s: float, ins: float, upd: float, dele: float) -> Tuple[float, float, float, float]:
    if min(s, ins, upd, dele) < 0:
        raise SystemExit("CRUD ratios (--select-ratio, --insert-ratio, --update-ratio, --delete-ratio) must be >= 0")
    t = s + ins + upd + dele
    if t <= 0:
        raise SystemExit("At least one CRUD ratio must be > 0")
    return (s / t, ins / t, upd / t, dele / t)


def physical_table_names(stem: str, num_tables: int) -> List[str]:
    _check_identifier(stem, "table name / stem (--table)")
    if num_tables < 1:
        raise SystemExit("--tables must be >= 1")
    if num_tables == 1:
        return [stem]
    names = [f"{stem}{i}" for i in range(1, num_tables + 1)]
    for n in names:
        _check_identifier(n, "generated table name")
    return names


def ddl_for_table(scheme: str, table: str) -> str:
    if scheme in ("postgresql", "postgres"):
        return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            payload TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{table}_created ON {table} (created_at DESC);
        """
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        payload TEXT NOT NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        INDEX idx_{table}_created (created_at)
    );
    """


def _fill_table_postgres(cur, table: str, n: int) -> None:
    cur.execute(
        f"INSERT INTO {table} (payload) SELECT md5(random()::text) "
        f"FROM generate_series(1, %s)",
        (n,),
    )


def _fill_table_mysql(cur, table: str, n: int) -> None:
    off = 0
    while off < n:
        m = min(_MYSQL_FILL_CHUNK, n - off)
        rows = [(f"b{off + i}",) for i in range(m)]
        cur.executemany(f"INSERT INTO {table} (payload) VALUES (%s)", rows)
        off += m


def prepare_benchmark(conn, scheme: str, table_names: List[str], table_size: int) -> None:
    cur = conn.cursor()
    try:
        for t in table_names:
            for stmt in ddl_for_table(scheme, t).split(";"):
                s = stmt.strip()
                if s:
                    cur.execute(s)
            conn.commit()
            if table_size <= 0:
                continue
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            cnt = cur.fetchone()[0]
            if cnt > 0:
                continue
            if scheme in ("postgresql", "postgres"):
                _fill_table_postgres(cur, t, table_size)
            else:
                _fill_table_mysql(cur, t, table_size)
            conn.commit()
    finally:
        cur.close()


def _pick_crud_op(u: float, crud: Tuple[float, float, float, float]) -> str:
    s, ins, upd, dele = crud
    if u < s:
        return "select"
    if u < s + ins:
        return "insert"
    if u < s + ins + upd:
        return "update"
    return "delete"


def _execute_crud_op(
    cur,
    scheme: str,
    op: str,
    tbl: str,
    pk_hi: Dict[str, int],
    randint,
    fixed_rid: Optional[int],
) -> None:
    """Run one statement. If fixed_rid is set, SELECT/UPDATE/DELETE use that id (multi-stmt txn)."""
    top = pk_hi.get(tbl, 0)

    if op == "select":
        if fixed_rid is not None:
            cur.execute(
                f"SELECT id, payload FROM {tbl} WHERE id = %s",
                (fixed_rid,),
            )
            cur.fetchone()
        elif top < 1:
            cur.execute(f"SELECT id, payload FROM {tbl} ORDER BY id DESC LIMIT 1")
            cur.fetchone()
        else:
            rid = randint(1, top)
            cur.execute(
                f"SELECT id, payload FROM {tbl} WHERE id = %s",
                (rid,),
            )
            cur.fetchone()
    elif op == "insert":
        payload = f"w{threading.get_ident()}-{time.time_ns()}"
        if scheme in ("postgresql", "postgres"):
            cur.execute(
                f"INSERT INTO {tbl} (payload) VALUES (%s) RETURNING id",
                (payload,),
            )
            row = cur.fetchone()
            if row:
                new_id = int(row[0])
                if new_id > pk_hi[tbl]:
                    pk_hi[tbl] = new_id
        else:
            cur.execute(
                f"INSERT INTO {tbl} (payload) VALUES (%s)",
                (payload,),
            )
            lid = cur.lastrowid
            if lid and lid > pk_hi[tbl]:
                pk_hi[tbl] = int(lid)
    elif op == "update":
        pl = f"u{threading.get_ident()}-{time.time_ns()}"
        if fixed_rid is not None:
            cur.execute(
                f"UPDATE {tbl} SET payload = %s WHERE id = %s",
                (pl, fixed_rid),
            )
        elif top < 1:
            cur.execute(
                f"UPDATE {tbl} SET payload = %s WHERE id = (SELECT MAX(id) FROM {tbl})",
                (pl,),
            )
        else:
            rid = randint(1, top)
            cur.execute(
                f"UPDATE {tbl} SET payload = %s WHERE id = %s",
                (pl, rid),
            )
    else:
        if fixed_rid is not None:
            cur.execute(f"DELETE FROM {tbl} WHERE id = %s", (fixed_rid,))
        elif top < 1:
            cur.execute(
                f"DELETE FROM {tbl} WHERE id = (SELECT MAX(id) FROM {tbl})",
            )
        else:
            rid = randint(1, top)
            cur.execute(f"DELETE FROM {tbl} WHERE id = %s", (rid,))


def worker_loop(
    scheme: str,
    kwargs: dict,
    table_names: List[str],
    initial_row_count: int,
    crud: Tuple[float, float, float, float],
    txn_mode: str,
    txn_statements: int,
    txn_crud: Tuple[float, float, float, float],
    end_time: float,
    result: WorkerResult,
    live: Optional[LiveStats] = None,
) -> None:
    conn = None
    connect_fail_streak = 0

    rnd = threading.local()

    def _rng() -> random.Random:
        if not hasattr(rnd, "g"):
            rnd.g = random.Random()
        return rnd.g

    def rand() -> float:
        return _rng().random()

    def randint(a: int, b: int) -> int:
        return _rng().randint(a, b)

    pk_hi = {t: initial_row_count for t in table_names}
    nt = len(table_names)
    txn_multi = txn_mode == "multi"
    is_pg = scheme in ("postgresql", "postgres")

    try:
        while time.monotonic() < end_time:
            if conn is None:
                try:
                    conn = _open_connection(scheme, kwargs)
                    connect_fail_streak = 0
                    if not is_pg and txn_multi:
                        conn.autocommit(False)
                except Exception:
                    connect_fail_streak += 1
                    # Back off so many workers do not busy-spin while the server is down.
                    delay = min(1.0, 0.05 * (2 ** min(connect_fail_streak, 6)))
                    time.sleep(delay)
                    continue

            t0 = time.perf_counter()
            cur = None
            try:
                cur = conn.cursor()
                if txn_multi:
                    if not is_pg:
                        cur.execute("START TRANSACTION")
                    tbl = table_names[randint(0, nt - 1)]
                    ttop = pk_hi.get(tbl, 0)
                    txn_rid = randint(1, ttop) if ttop >= 1 else None
                    for _ in range(txn_statements):
                        op = _pick_crud_op(rand(), txn_crud)
                        _execute_crud_op(cur, scheme, op, tbl, pk_hi, randint, txn_rid)
                    conn.commit()
                else:
                    op = _pick_crud_op(rand(), crud)
                    tbl = table_names[randint(0, nt - 1)]
                    _execute_crud_op(cur, scheme, op, tbl, pk_hi, randint, None)
                    if is_pg:
                        conn.commit()
                ms = (time.perf_counter() - t0) * 1000.0
                result.ops += 1
                result.latencies_ms.append(ms)
                if live is not None:
                    live.record_op(ms)
            except Exception as e:
                result.errors += 1
                if live is not None:
                    live.record_error()
                if _is_connection_level_error(e, scheme):
                    _safe_close_conn(conn)
                    conn = None
                    time.sleep(0.05)
                else:
                    try:
                        conn.rollback()
                    except Exception:
                        _safe_close_conn(conn)
                        conn = None
            finally:
                if cur is not None:
                    try:
                        cur.close()
                    except Exception:
                        pass
    finally:
        _safe_close_conn(conn)


def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def merge_results(parts: List[WorkerResult]) -> WorkerResult:
    out = WorkerResult()
    for p in parts:
        out.ops += p.ops
        out.errors += p.errors
        out.latencies_ms.extend(p.latencies_ms)
    return out


def periodic_report_loop(live: LiveStats, interval_sec: float, stop: threading.Event) -> None:
    last_ops = 0
    last_errors = 0
    last_t = time.perf_counter()
    while True:
        signaled = stop.wait(timeout=interval_sec)
        now = time.perf_counter()
        wall = max(now - last_t, 1e-9)
        cum_ops, cum_errs, lats = live.snapshot_interval()
        d_o = cum_ops - last_ops
        d_e = cum_errs - last_errors
        last_ops, last_errors = cum_ops, cum_errs
        last_t = now
        parts = [
            f"[interval {wall:.2f}s]",
            f"ops={d_o}",
            f"errors={d_e}",
            f"qps={d_o / wall:.2f}",
        ]
        if lats:
            sl = sorted(lats)
            parts.append(f"latency_ms_mean={statistics.mean(sl):.3f}")
            parts.append(f"p50={percentile(sl, 50):.3f}")
            parts.append(f"p95={percentile(sl, 95):.3f}")
            parts.append(f"p99={percentile(sl, 99):.3f}")
        print(" ".join(parts), flush=True)
        if signaled:
            break


def run_bench(cfg: BenchConfig) -> WorkerResult:
    scheme, kwargs = parse_db_url(cfg.url)
    if scheme == "postgres":
        scheme = "postgresql"

    table_names = physical_table_names(cfg.table, cfg.tables)
    crud = (cfg.select_ratio, cfg.insert_ratio, cfg.update_ratio, cfg.delete_ratio)
    txn_crud = (
        cfg.txn_select_ratio,
        cfg.txn_insert_ratio,
        cfg.txn_update_ratio,
        cfg.txn_delete_ratio,
    )

    if cfg.prepare:
        conn = connect_postgres(kwargs) if scheme == "postgresql" else connect_mysql(kwargs)
        try:
            prepare_benchmark(conn, scheme, table_names, cfg.table_size)
        finally:
            conn.close()

    results: List[WorkerResult] = [WorkerResult() for _ in range(cfg.workers)]

    if cfg.warmup_sec > 0:
        warm_end = time.monotonic() + cfg.warmup_sec
        with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
            futs = [
                ex.submit(
                    worker_loop,
                    scheme,
                    kwargs,
                    table_names,
                    cfg.table_size,
                    crud,
                    cfg.txn_mode,
                    cfg.txn_statements,
                    txn_crud,
                    warm_end,
                    results[i],
                )
                for i in range(cfg.workers)
            ]
            for f in as_completed(futs):
                f.result()
        for r in results:
            r.ops = 0
            r.errors = 0
            r.latencies_ms.clear()

    end = time.monotonic() + cfg.duration_sec
    live: Optional[LiveStats] = None
    stop_report: Optional[threading.Event] = None
    rep_thread: Optional[threading.Thread] = None
    if cfg.report_interval_sec > 0:
        live = LiveStats()
        stop_report = threading.Event()
        rep_thread = threading.Thread(
            target=periodic_report_loop,
            args=(live, cfg.report_interval_sec, stop_report),
            name="db-bench-report",
            daemon=True,
        )
        rep_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
            futs = [
                ex.submit(
                    worker_loop,
                    scheme,
                    kwargs,
                    table_names,
                    cfg.table_size,
                    crud,
                    cfg.txn_mode,
                    cfg.txn_statements,
                    txn_crud,
                    end,
                    results[i],
                    live,
                )
                for i in range(cfg.workers)
            ]
            for f in as_completed(futs):
                f.result()
    finally:
        if stop_report is not None:
            stop_report.set()
        if rep_thread is not None:
            rep_thread.join(timeout=max(cfg.report_interval_sec * 2, 2.0))

    return merge_results(results)


def print_report(total: WorkerResult, duration_sec: float, workload_line: str = "") -> None:
    lat = sorted(total.latencies_ms)
    n = len(lat)
    if workload_line:
        print(workload_line)
    print(f"duration_s: {duration_sec:.3f}")
    print(f"operations: {total.ops}  errors: {total.errors}")
    if duration_sec > 0:
        print(f"throughput_ops_s: {total.ops / duration_sec:.2f}")
    if n:
        print(f"latency_ms: min={lat[0]:.3f}  max={lat[-1]:.3f}  mean={statistics.mean(lat):.3f}")
        for p in (50, 95, 99):
            print(f"latency_ms_p{p}: {percentile(lat, p):.3f}")


def main() -> None:
    cfg_path, argv_rest = extract_config_path(sys.argv[1:])
    file_cfg: dict = {}
    if cfg_path:
        file_cfg = normalize_config_mapping(load_config_file(cfg_path))
    merged = merge_config_with_program_defaults(file_cfg)
    p = build_argument_parser(merged)
    args = p.parse_args(argv_rest)
    if args.no_prepare:
        args.prepare = False
    if not args.url or not str(args.url).strip():
        raise SystemExit("Database URL is required (config key 'url' or --url).")
    if args.report_interval < 0:
        raise SystemExit("--report-interval must be >= 0")
    if args.report_interval > 0 and args.report_interval < 0.1:
        raise SystemExit("--report-interval must be >= 0.1 or 0")
    if args.tables < 1:
        raise SystemExit("--tables must be >= 1")
    if args.table_size < 0:
        raise SystemExit("--table-size must be >= 0")
    if args.txn_mode == "multi" and args.txn_statements < 1:
        raise SystemExit("--txn-statements must be >= 1 when --txn-mode multi")

    if args.mode == "read":
        s, ins, upd, dele = 1.0, 0.0, 0.0, 0.0
    elif args.mode == "write":
        s, ins, upd, dele = 0.0, 1.0, 0.0, 0.0
    else:
        s, ins, upd, dele = (
            args.select_ratio,
            args.insert_ratio,
            args.update_ratio,
            args.delete_ratio,
        )
    s, ins, upd, dele = normalize_crud_ratios(s, ins, upd, dele)

    if args.mode == "read":
        ts, tins, tupd, tdel = 1.0, 0.0, 0.0, 0.0
    elif args.mode == "write":
        ts, tins, tupd, tdel = 0.0, 1.0, 0.0, 0.0
    else:
        ts, tins, tupd, tdel = (
            args.txn_select_ratio,
            args.txn_insert_ratio,
            args.txn_update_ratio,
            args.txn_delete_ratio,
        )
    ts, tins, tupd, tdel = normalize_crud_ratios(ts, tins, tupd, tdel)

    cfg = BenchConfig(
        url=args.url,
        workers=max(1, args.workers),
        duration_sec=max(0.01, args.duration),
        select_ratio=s,
        insert_ratio=ins,
        update_ratio=upd,
        delete_ratio=dele,
        txn_mode=args.txn_mode,
        txn_statements=max(1, args.txn_statements),
        txn_select_ratio=ts,
        txn_insert_ratio=tins,
        txn_update_ratio=tupd,
        txn_delete_ratio=tdel,
        table=args.table,
        tables=args.tables,
        table_size=args.table_size,
        prepare=args.prepare,
        warmup_sec=max(0.0, args.warmup),
        report_interval_sec=args.report_interval,
    )

    t0 = time.perf_counter()
    total = run_bench(cfg)
    elapsed = time.perf_counter() - t0
    # Use configured duration for throughput (steady window); fallback to wall clock
    measure_sec = cfg.duration_sec if cfg.warmup_sec == 0 else cfg.duration_sec
    if args.mode == "oltp":
        wl = (
            f"workload: oltp crud="
            f"sel={s:.3f} ins={ins:.3f} upd={upd:.3f} del={dele:.3f}"
        )
    elif args.mode == "read":
        wl = "workload: read-only (select=1)"
    else:
        wl = "workload: write-only (insert=1)"
    wl += f" | txn_mode={cfg.txn_mode}"
    if cfg.txn_mode == "multi":
        wl += (
            f" txn_stmts={cfg.txn_statements} "
            f"txn_crud=sel={ts:.3f} ins={tins:.3f} upd={tupd:.3f} del={tdel:.3f}"
        )
    wl += (
        f" | tables={cfg.tables} table_size={cfg.table_size} "
        f"(stem={cfg.table!r})"
    )
    if cfg.txn_mode == "multi":
        wl += " | note: operations/throughput = completed transactions (not single SQLs)"
    print_report(total, measure_sec, wl)


if __name__ == "__main__":
    main()
