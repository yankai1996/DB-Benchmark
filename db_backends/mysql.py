"""MySQL backend (PyMySQL)."""

from __future__ import annotations

from typing import List, Optional

from db_backends.base import Backend
from bench_model import BenchModel, ExtraColumn, physical_index_name

_CONNECT_TIMEOUT_SEC = 10
_MYSQL_SOCK_TIMEOUT_SEC = 30
_MYSQL_FILL_CHUNK = 1000


class MySQLBackend(Backend):
    def connect(self, kwargs: dict):
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

    def is_connection_error(self, exc: BaseException) -> bool:
        import pymysql.err

        return isinstance(exc, (pymysql.err.OperationalError, pymysql.err.InterfaceError))

    def default_ddl(self, table: str) -> str:
        return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        payload TEXT NOT NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        INDEX idx_{table}_created (created_at)
    );
    """

    def prepare_model_statements(self, table: str, model: BenchModel) -> List[str]:
        body = [
            "id BIGINT AUTO_INCREMENT PRIMARY KEY",
            "payload TEXT NOT NULL",
            "created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
        ]
        body.extend(self.render_extra_column_definition(e) for e in model.extra_columns)
        lines = [
            "CREATE TABLE IF NOT EXISTS "
            + table
            + " (\n    "
            + ",\n    ".join(body)
            + "\n);"
        ]
        for ix in model.indexes:
            iname = physical_index_name(table, ix.name)
            cols = ", ".join(ix.columns)
            u = "UNIQUE " if ix.unique else ""
            lines.append(f"CREATE {u}INDEX IF NOT EXISTS {iname} ON {table} ({cols});")
        return lines

    def _default_sql_literal(self, col: ExtraColumn) -> str:
        assert col.not_null and col.default is not None
        if col.sql_kind in ("int", "bigint"):
            return str(int(col.default))
        return "'" + str(col.default).replace("'", "''") + "'"

    def render_extra_column_definition(self, col: ExtraColumn) -> str:
        n = col.name
        if col.sql_kind == "int":
            t = "INT"
        elif col.sql_kind == "bigint":
            t = "BIGINT"
        elif col.sql_kind == "text":
            t = "TEXT"
        elif col.sql_kind == "varchar":
            t = f"VARCHAR({col.varchar_length})"
        else:
            raise SystemExit(f"Unknown sql_kind for column {n!r}: {col.sql_kind!r}")
        if col.not_null:
            return f"{n} {t} NOT NULL DEFAULT {self._default_sql_literal(col)}"
        return f"{n} {t} NULL"

    def fill_table(self, cur, table: str, n: int) -> None:
        off = 0
        while off < n:
            m = min(_MYSQL_FILL_CHUNK, n - off)
            rows = [(f"b{off + i}",) for i in range(m)]
            cur.executemany(f"INSERT INTO {table} (payload) VALUES (%s)", rows)
            off += m

    def format_created_at_assignment(self) -> str:
        return "created_at = CURRENT_TIMESTAMP(6)"

    def run_insert_returning_id(self, cur, table: str, payload: str) -> Optional[int]:
        cur.execute(
            f"INSERT INTO {table} (payload) VALUES (%s)",
            (payload,),
        )
        lid = cur.lastrowid
        return int(lid) if lid else None

    def on_worker_connect(self, conn, txn_multi: bool) -> None:
        if txn_multi:
            conn.autocommit(False)

    def begin_multi_statement_transaction(self, cur) -> None:
        cur.execute("START TRANSACTION")

    def commit_after_each_statement_single_mode(self) -> bool:
        return False


mysql_backend = MySQLBackend()
