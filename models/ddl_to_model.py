#!/usr/bin/env python3
"""
Convert a small subset of DDL (CREATE TABLE / CREATE INDEX) into a bench model YAML.

Recognized column types map to model: int, bigint, text, varchar(n).
Any other type (e.g. TIMESTAMP, JSON, DECIMAL) is emitted as varchar(128) with a stderr warning.
Built-in columns id, k, c, pad are skipped. SERIAL/BIGSERIAL/GENERATED lines are still skipped.

Indexes: only standalone ``CREATE [UNIQUE] INDEX ... ON table (cols)`` — not inline
``INDEX (...)`` inside ``CREATE TABLE`` (MySQL).

Usage:
  python3 models/ddl_to_model.py -i schema.sql -o models/out.yaml
  python3 models/ddl_to_model.py < schema.sql
"""

from __future__ import annotations

import argparse
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

_BUILTIN = frozenset({"id", "k", "c", "pad"})


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    out_lines: List[str] = []
    for line in sql.splitlines():
        i = 0
        cur: List[str] = []
        in_s = False
        in_d = False
        while i < len(line):
            c = line[i]
            if not in_s and not in_d and c == "-" and i + 1 < len(line) and line[i + 1] == "-":
                break
            if not in_d and c == "'":
                in_s = not in_s
                cur.append(c)
            elif not in_s and c == '"':
                in_d = not in_d
                cur.append(c)
            else:
                cur.append(c)
            i += 1
        out_lines.append("".join(cur))
    return "\n".join(out_lines)


def _split_top_level_commas(s: str) -> List[str]:
    parts: List[str] = []
    depth = 0
    cur: List[str] = []
    in_s = False
    in_d = False
    esc = False
    i = 0
    while i < len(s):
        c = s[i]
        if esc:
            cur.append(c)
            esc = False
            i += 1
            continue
        if in_s:
            cur.append(c)
            if c == "'":
                in_s = False
            i += 1
            continue
        if in_d:
            cur.append(c)
            if c == '"':
                in_d = False
            i += 1
            continue
        if c == "\\" and (in_s or in_d):
            esc = True
            cur.append(c)
            i += 1
            continue
        if c == "'":
            in_s = True
            cur.append(c)
            i += 1
            continue
        if c == '"':
            in_d = True
            cur.append(c)
            i += 1
            continue
        if c == "(":
            depth += 1
            cur.append(c)
        elif c == ")":
            depth -= 1
            cur.append(c)
        elif c == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
        i += 1
    if cur:
        parts.append("".join(cur).strip())
    return [p for p in parts if p]


_SKIP_LINE = re.compile(
    r"^\s*(PRIMARY\s+KEY|UNIQUE\s*\(|CONSTRAINT\b|FOREIGN\s+KEY|CHECK\s*\()",
    re.IGNORECASE,
)


def _parse_column_line(line: str) -> Optional[Dict[str, Any]]:
    line = line.strip().rstrip(",")
    if not line or _SKIP_LINE.match(line):
        return None
    m = re.match(
        r'^"?([A-Za-z_][A-Za-z0-9_]*)"?\s+(.+)$',
        line,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return None
    name, rest = m.group(1).lower(), m.group(2).strip()
    if name in _BUILTIN:
        return None

    rest_up = rest.upper()
    not_null = bool(re.search(r"\bNOT\s+NULL\b", rest_up))
    typ: Optional[str] = None
    extra: Dict[str, Any] = {}

    if re.search(r"\b(SERIAL|BIGSERIAL)\b", rest_up) or re.search(
        r"\bGENERATED\b", rest_up
    ):
        return None

    if re.search(r"^\s*INT8\b", rest_up):
        typ = "bigint"
    elif re.search(r"^\s*(INTEGER|INT4)\b", rest_up) or (
        re.search(r"^\s*INT\b", rest_up)
        and not re.search(r"^\s*INTEGER\b", rest_up)
    ):
        typ = "int"
    elif re.search(r"^\s*BIGINT\b", rest_up):
        typ = "bigint"
    elif re.search(r"^\s*TEXT\b", rest_up):
        typ = "text"
    else:
        vm = re.search(
            r"^\s*(?:CHARACTER\s+VARYING|VARCHAR)\s*\(\s*(\d+)\s*\)",
            rest_up,
            re.IGNORECASE,
        )
        if vm:
            typ = "varchar"
            extra["length"] = int(vm.group(1))

    coerced_to_varchar = False
    if typ is None:
        typ = "varchar"
        extra["length"] = 128
        coerced_to_varchar = True

    default_val: Any = None
    dm = re.search(
        r"\bDEFAULT\s+((?:-?\d+)|(?:'(?:[^']|'')*')|(?:\"(?:[^\"]|\"\")*\"))",
        rest,
        re.IGNORECASE | re.DOTALL,
    )
    if dm:
        raw = dm.group(1).strip()
        if raw.startswith("'") and raw.endswith("'"):
            default_val = raw[1:-1].replace("''", "'")
        elif raw.startswith('"') and raw.endswith('"'):
            default_val = raw[1:-1].replace('""', '"')
        else:
            try:
                default_val = int(raw)
            except ValueError:
                default_val = raw

    col: Dict[str, Any] = {"name": name, "type": typ}
    if typ == "varchar" and "length" in extra:
        col["length"] = extra["length"]
    if coerced_to_varchar:
        col["_coerced_unsupported_type"] = rest[:200]
    if not_null:
        col["not_null"] = True
        if default_val is not None:
            col["default"] = default_val
        else:
            col["_missing_default"] = True
    if coerced_to_varchar and col.get("not_null") and col.get("default") is not None:
        if not isinstance(col["default"], str):
            col["default"] = str(col["default"])
    return col


def _extract_create_table_body(sql: str) -> Optional[str]:
    m = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\s*(.+?)\s*\(",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None
    open_paren = m.end() - 1
    if open_paren < 0 or sql[open_paren] != "(":
        return None
    depth = 0
    i = open_paren
    in_s = False
    in_d = False
    while i < len(sql):
        c = sql[i]
        if not in_s and not in_d:
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    return sql[open_paren + 1 : i].strip()
        if not in_d and c == "'":
            in_s = not in_s
        elif not in_s and c == '"':
            in_d = not in_d
        i += 1
    return None


def _parse_indexes(sql: str) -> List[Dict[str, Any]]:
    indexes: List[Dict[str, Any]] = []
    for stmt in re.split(r";+", sql):
        stmt = stmt.strip()
        if not stmt or not re.search(r"\bCREATE\s+.*\bINDEX\b", stmt, re.IGNORECASE):
            continue
        m = re.search(
            r"CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?"
            r"[`\"]?(\w+)[`\"]?\s+ON\s+[`\"]?[\w.]+[`\"]?\s*\(\s*([^)]+)\s*\)",
            stmt,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            continue
        unique = bool(m.group(1) and m.group(1).strip())
        idx_name = m.group(2)
        cols_raw = m.group(3)
        cols = [c.strip().strip("`").strip('"') for c in cols_raw.split(",") if c.strip()]
        if not idx_name or not cols:
            continue
        entry: Dict[str, Any] = {"name": idx_name, "columns": cols}
        if unique:
            entry["unique"] = True
        indexes.append(entry)
    return indexes


def ddl_to_model_dict(sql: str) -> Tuple[Dict[str, Any], List[str]]:
    sql = _strip_comments(sql)
    warnings: List[str] = []

    body = _extract_create_table_body(sql)
    extra_columns: List[Dict[str, Any]] = []
    if body:
        for part in _split_top_level_commas(body):
            col = _parse_column_line(part)
            if not col:
                continue
            raw_coerce = col.pop("_coerced_unsupported_type", None)
            if raw_coerce is not None:
                warnings.append(
                    f"column {col['name']!r}: unsupported DDL type, coerced to varchar(128) ({raw_coerce!r})"
                )
            if col.pop("_missing_default", None):
                warnings.append(
                    f"column {col['name']!r}: NOT NULL without DEFAULT in DDL — "
                    f"add `default` in output or make nullable"
                )
            extra_columns.append(col)

    indexes = _parse_indexes(sql)

    run_cols = ["k", "c"] + [c["name"] for c in extra_columns if "name" in c]

    model: Dict[str, Any] = {
        "prepare": {
            "extra_columns": extra_columns,
            "indexes": indexes,
        },
        "run": {"update_columns": run_cols},
    }
    return model, warnings


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Convert CREATE TABLE / CREATE INDEX DDL to a db_bench model YAML.",
        epilog="Limitations: id/k/c/pad skipped; SERIAL/BIGSERIAL/GENERATED skipped; "
        "other unknown types become varchar(128) with a warning; inline INDEX in CREATE TABLE not supported.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "-i",
        "--input",
        metavar="FILE.sql",
        help="DDL file (default: stdin)",
    )
    ap.add_argument(
        "-o",
        "--output",
        metavar="FILE.yaml",
        help="Write YAML here (default: stdout)",
    )
    ap.add_argument(
        "--no-run",
        action="store_true",
        help="Omit run.update_columns (prepare only)",
    )
    args = ap.parse_args()

    if args.input:
        with open(args.input, encoding="utf-8") as f:
            sql = f.read()
    else:
        sql = sys.stdin.read()

    model, warnings = ddl_to_model_dict(sql)
    if args.no_run:
        del model["run"]

    for w in warnings:
        print(f"warning: {w}", file=sys.stderr)

    try:
        import yaml
    except ImportError as e:
        raise SystemExit("PyYAML is required: pip install pyyaml") from e

    out = yaml.dump(
        model,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
    )
    header = (
        "# Generated by models/ddl_to_model.py — review before use.\n"
        "# Table stem / fill: CLI --table, --tables, --table-size.\n\n"
    )
    text = header + out

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
