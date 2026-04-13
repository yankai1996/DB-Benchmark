"""PostgreSQL backend (psycopg2)."""

from __future__ import annotations

from typing import List, Optional

from db_backends.base import Backend
from bench_model import BenchModel, ExtraColumn, physical_index_name

_CONNECT_TIMEOUT_SEC = 10


class PostgreSQLBackend(Backend):
    def connect(self, kwargs: dict):
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

    def is_connection_error(self, exc: BaseException) -> bool:
        import psycopg2

        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))

    def default_ddl(self, table: str) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            k BIGINT NOT NULL DEFAULT 0,
            c CHAR(120) NOT NULL DEFAULT '',
            pad CHAR(60) NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_{table}_k ON {table} (k);
        """

    def prepare_model_statements(self, table: str, model: BenchModel) -> List[str]:
        body = [
            "id BIGSERIAL PRIMARY KEY",
            "k BIGINT NOT NULL DEFAULT 0",
            "c CHAR(120) NOT NULL DEFAULT ''",
            "pad CHAR(60) NOT NULL DEFAULT ''",
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
            t = "INTEGER"
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
        cur.execute(
            f"INSERT INTO {table} (k, c, pad) "
            f"SELECT floor(random() * %s)::bigint + 1, "
            f"left(md5(random()::text), 120), left(md5(random()::text), 60) "
            f"FROM generate_series(1, %s)",
            (max(1, n), n),
        )

    def run_insert_returning_id(self, cur, table: str, k: int, c: str, pad: str) -> Optional[int]:
        cur.execute(
            f"INSERT INTO {table} (k, c, pad) VALUES (%s, %s, %s) RETURNING id",
            (k, c, pad),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])
        return None

    def on_worker_connect(self, conn, txn_multi: bool) -> None:
        _ = conn, txn_multi

    def begin_multi_statement_transaction(self, cur) -> None:
        _ = cur

    def commit_after_each_statement_single_mode(self) -> bool:
        return True


postgresql_backend = PostgreSQLBackend()
