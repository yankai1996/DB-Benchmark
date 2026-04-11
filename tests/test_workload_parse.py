"""Unit tests for workload parsing (no database)."""

import unittest

from bench_model import parse_workload, default_workload_spec


class TestWorkloadMix(unittest.TestCase):
    def test_default(self) -> None:
        w = parse_workload(None)
        self.assertEqual(w.statements, ())
        self.assertEqual(w.mix_single[0], 0.5)

    def test_mix_single(self) -> None:
        w = parse_workload(
            {
                "transaction": {"shape": "single"},
                "mix": {"single": {"select": 1, "insert": 0, "update": 0, "delete": 0}},
            }
        )
        self.assertEqual(w.statements, ())
        self.assertEqual(w.mix_single, (1.0, 0.0, 0.0, 0.0))


class TestWorkloadTemplates(unittest.TestCase):
    def test_template_track_forbids_mix(self) -> None:
        with self.assertRaises(SystemExit):
            parse_workload(
                {
                    "transaction": {"shape": "single"},
                    "mix": {"single": {"select": 1, "insert": 0, "update": 0, "delete": 0}},
                    "statements": [{"id": "a", "weight": 1, "kind": "select"}],
                }
            )

    def test_templates_builtin_only(self) -> None:
        w = parse_workload(
            {
                "transaction": {"shape": "single"},
                "statements": [
                    {"id": "a", "weight": 3, "kind": "select"},
                    {"id": "b", "weight": 1, "kind": "insert"},
                ],
            }
        )
        self.assertEqual(len(w.statements), 2)
        self.assertEqual(w.statements[0].kind, "select")
        self.assertIsNone(w.statements[0].sql)

    def test_templates_custom_sql(self) -> None:
        w = parse_workload(
            {
                "transaction": {"shape": "single"},
                "statements": [
                    {
                        "id": "custom_sel",
                        "weight": 1,
                        "kind": "select",
                        "sql": "SELECT {select_list} FROM {table} WHERE id = %s",
                    },
                ],
            }
        )
        self.assertEqual(w.statements[0].sql, "SELECT {select_list} FROM {table} WHERE id = %s")

    def test_workload_range_size(self) -> None:
        w = parse_workload(
            {
                "transaction": {"shape": "single"},
                "range_size": 50,
                "statements": [{"id": "a", "weight": 1, "kind": "select"}],
            }
        )
        self.assertEqual(w.range_size, 50)

    def test_statement_range_size_override(self) -> None:
        w = parse_workload(
            {
                "transaction": {"shape": "single"},
                "range_size": 100,
                "statements": [
                    {
                        "id": "r",
                        "weight": 1,
                        "kind": "select",
                        "sql": "SELECT 1 FROM t WHERE id BETWEEN %s AND %s",
                        "range_size": 10,
                    },
                ],
            }
        )
        self.assertEqual(w.statements[0].range_size, 10)


if __name__ == "__main__":
    unittest.main()
