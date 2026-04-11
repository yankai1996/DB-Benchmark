"""Runtime checks for template SQL helpers (no real database)."""

import unittest
from typing import Any, List, Optional, Tuple

from bench_model import StatementTemplate

from db_bench import _count_s_placeholders, execute_statement_template


class _MockCursor:
    def __init__(self) -> None:
        self.executes: List[Tuple[str, Optional[Tuple[Any, ...]]]] = []
        self.lastrowid: Optional[int] = None
        self.description: Any = None

    def execute(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        self.executes.append((sql, params))

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return (1,)


class TestCountSPlaceholders(unittest.TestCase):
    def test_simple(self) -> None:
        self.assertEqual(_count_s_placeholders("SELECT * FROM t WHERE id = %s"), 1)
        self.assertEqual(_count_s_placeholders("SELECT %s, %s FROM t"), 2)

    def test_percent_escape(self) -> None:
        # %% is literal percent, not a placeholder
        self.assertEqual(_count_s_placeholders("LIKE '100%%' AND id = %s"), 1)


class TestExecuteStatementTemplate(unittest.TestCase):
    def test_select_one_param(self) -> None:
        cur = _MockCursor()
        tmpl = StatementTemplate(
            "s1",
            1.0,
            "select",
            "SELECT {select_list} FROM {table} WHERE id = %s",
            None,
        )
        pk_hi = {"t1": 10}

        def ri(a: int, b: int) -> int:
            return 3

        execute_statement_template(cur, tmpl, "t1", pk_hi, ri, None, "id, payload", 100)
        self.assertEqual(len(cur.executes), 1)
        sql, params = cur.executes[0]
        self.assertIn("FROM t1", sql)
        self.assertIn("id, payload", sql)
        self.assertEqual(params, (3,))

    def test_select_range_two_params(self) -> None:
        cur = _MockCursor()
        tmpl = StatementTemplate(
            "sr",
            1.0,
            "select",
            "SELECT {select_list} FROM {table} WHERE id BETWEEN %s AND %s",
            None,
        )
        pk_hi = {"t1": 500}

        def ri(a: int, b: int) -> int:
            return 42

        execute_statement_template(cur, tmpl, "t1", pk_hi, ri, None, "id, payload", 100)
        self.assertEqual(len(cur.executes), 1)
        _sql, params = cur.executes[0]
        self.assertIsNotNone(params)
        assert params is not None
        lo, hi = params[0], params[1]
        self.assertEqual((lo, hi), (42, 141))

    def test_insert_one_param_updates_pk_via_lastrowid(self) -> None:
        cur = _MockCursor()
        cur.lastrowid = 42
        tmpl = StatementTemplate(
            "i1", 1.0, "insert", "INSERT INTO {table} (payload) VALUES (%s)", None
        )
        pk_hi: dict = {"t1": 0}

        execute_statement_template(cur, tmpl, "t1", pk_hi, lambda a, b: 1, None, "id", 100)
        self.assertEqual(pk_hi["t1"], 42)

    def test_select_rid_missing_raises(self) -> None:
        cur = _MockCursor()
        tmpl = StatementTemplate(
            "s1", 1.0, "select", "SELECT 1 FROM {table} WHERE id = %s", None
        )
        pk_hi = {"t1": 0}

        with self.assertRaises(RuntimeError):
            execute_statement_template(
                cur, tmpl, "t1", pk_hi, lambda a, b: 1, None, "id", 100
            )


if __name__ == "__main__":
    unittest.main()
