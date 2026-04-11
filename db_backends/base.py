"""Database backend abstract base for db_bench (prepare + run workload)."""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from bench_model import ExtraColumn

if TYPE_CHECKING:
    from bench_model import BenchModel


def workload_select_columns(update_columns: Tuple[str, ...]) -> str:
    """SELECT list: id first, then columns from run.update_columns (deduped)."""
    parts: List[str] = ["id"]
    seen = {"id"}
    for c in update_columns:
        if c in seen:
            continue
        seen.add(c)
        parts.append(c)
    return ", ".join(parts)


class Backend(ABC):
    """Pluggable dialect: connection, DDL, fills, INSERT semantics, transaction quirks."""

    @abstractmethod
    def connect(self, kwargs: dict) -> object:
        """Return a live DB-API connection."""

    @abstractmethod
    def is_connection_error(self, exc: BaseException) -> bool:
        """True if the session should be discarded (reconnect)."""

    @abstractmethod
    def default_ddl(self, table: str) -> str:
        """Semicolon-separated DDL for built-in benchmark table + default index(es)."""

    @abstractmethod
    def prepare_model_statements(self, table: str, model: "BenchModel") -> List[str]:
        """Ordered statements for --model prepare (CREATE TABLE + indexes)."""

    @abstractmethod
    def render_extra_column_definition(self, col: ExtraColumn) -> str:
        """Single line inside CREATE TABLE for an extra column from the model YAML."""

    @abstractmethod
    def fill_table(self, cur, table: str, n: int) -> None:
        """Insert n benchmark rows when table is empty (payload only)."""

    @abstractmethod
    def format_created_at_assignment(self) -> str:
        """Single SET clause fragment, e.g. ``created_at = NOW()`` (no leading comma)."""

    @abstractmethod
    def run_insert_returning_id(self, cur, table: str, payload: str) -> Optional[int]:
        """INSERT one row; return new primary key if known."""

    @abstractmethod
    def on_worker_connect(self, conn, txn_multi: bool) -> None:
        """e.g. MySQL multi: autocommit off."""

    @abstractmethod
    def begin_multi_statement_transaction(self, cur) -> None:
        """Start explicit txn if required (MySQL); no-op on PG implicit txn."""

    @abstractmethod
    def commit_after_each_statement_single_mode(self) -> bool:
        """True: caller must conn.commit() after each statement in single-stmt mode (PostgreSQL)."""

    def build_update_set_parts(
        self,
        update_columns: Tuple[str, ...],
        extra_by_name: Dict[str, ExtraColumn],
        payload_value: str,
        randint: Callable[[int, int], int],
    ) -> Tuple[List[str], List[object]]:
        """SET fragments and params for benchmark UPDATE (shared PG/MySQL SQL)."""
        parts: List[str] = []
        params: List[object] = []
        for col in update_columns:
            if col == "payload":
                parts.append("payload = %s")
                params.append(payload_value)
            elif col == "created_at":
                parts.append(self.format_created_at_assignment())
            else:
                ex = extra_by_name.get(col)
                if ex is None:
                    raise SystemExit(
                        f"Internal error: update column {col!r} has no model metadata "
                        "(use --model with prepare.extra_columns for non-built-in columns)"
                    )
                k = ex.sql_kind
                if k in ("int", "bigint"):
                    parts.append(f"{col} = %s")
                    params.append(randint(0, 2_147_483_647))
                elif k in ("text", "varchar"):
                    parts.append(f"{col} = %s")
                    params.append(f"u{col}-{threading.get_ident()}-{time.time_ns()}")
                else:
                    raise SystemExit(f"Unsupported extra column sql_kind for UPDATE: {k!r}")
        return parts, params

    def execute_workload_statement(
        self,
        cur,
        op: str,
        tbl: str,
        pk_hi: Dict[str, int],
        randint: Callable[[int, int], int],
        fixed_rid: Optional[int],
        update_columns: Tuple[str, ...],
        extra_by_name: Dict[str, ExtraColumn],
        select_list_sql: str,
    ) -> None:
        """Default benchmark SELECT / INSERT / UPDATE / DELETE (override if dialect differs).

        ``select_list_sql`` should be ``workload_select_columns(update_columns)`` computed once
        per worker (cached in worker_loop) to avoid rebuilding the SELECT list every statement.
        """
        top = pk_hi.get(tbl, 0)
        sel = select_list_sql

        if op == "select":
            if fixed_rid is not None:
                cur.execute(
                    f"SELECT {sel} FROM {tbl} WHERE id = %s",
                    (fixed_rid,),
                )
                cur.fetchone()
            elif top < 1:
                cur.execute(f"SELECT {sel} FROM {tbl} ORDER BY id DESC LIMIT 1")
                cur.fetchone()
            else:
                rid = randint(1, top)
                cur.execute(
                    f"SELECT {sel} FROM {tbl} WHERE id = %s",
                    (rid,),
                )
                cur.fetchone()
        elif op == "insert":
            payload = f"w{threading.get_ident()}-{time.time_ns()}"
            new_id = self.run_insert_returning_id(cur, tbl, payload)
            if new_id is not None and new_id > pk_hi[tbl]:
                pk_hi[tbl] = new_id
        elif op == "update":
            pl = f"u{threading.get_ident()}-{time.time_ns()}"
            parts, params = self.build_update_set_parts(
                update_columns, extra_by_name, pl, randint
            )
            set_sql = ", ".join(parts)
            if fixed_rid is not None:
                cur.execute(
                    f"UPDATE {tbl} SET {set_sql} WHERE id = %s",
                    tuple(params) + (fixed_rid,),
                )
            elif top < 1:
                cur.execute(
                    f"UPDATE {tbl} SET {set_sql} WHERE id = (SELECT MAX(id) FROM {tbl})",
                    tuple(params),
                )
            else:
                rid = randint(1, top)
                cur.execute(
                    f"UPDATE {tbl} SET {set_sql} WHERE id = %s",
                    tuple(params) + (rid,),
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

