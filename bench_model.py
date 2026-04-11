"""YAML model for `prepare --model` / `run --model`: DDL, indexes, and run.update_columns."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_IDENT_LEN = 63

_BUILTIN_COLS = frozenset({"id", "payload", "created_at"})


def _check_ident(name: str, what: str) -> None:
    if len(name) > _MAX_IDENT_LEN:
        raise SystemExit(f"{what} is too long (max {_MAX_IDENT_LEN} characters): {name!r}")
    if not _IDENT_RE.match(name):
        raise SystemExit(
            f"Invalid {what} {name!r}: use ASCII letters, digits, underscore; "
            "must start with a letter or underscore"
        )


@dataclass(frozen=True)
class ExtraColumn:
    """Logical column from prepare.extra_columns; DDL is rendered by Backend.render_extra_column_definition."""

    name: str
    sql_kind: str  # int | bigint | text | varchar
    varchar_length: int = 128
    not_null: bool = False
    default: Optional[object] = None


@dataclass(frozen=True)
class IndexSpec:
    """Logical index name (per table); emitted SQL uses `{table}_{name}` for PostgreSQL uniqueness."""

    name: str
    columns: Tuple[str, ...]
    unique: bool = False


@dataclass(frozen=True)
class BenchModel:
    """prepare DDL plus run workload; run_extensions stores unknown keys under `run` for future use."""

    extra_columns: Tuple[ExtraColumn, ...]
    indexes: Tuple[IndexSpec, ...]
    run_update_columns: Tuple[str, ...]
    run_extensions: Tuple[Tuple[str, Any], ...]


def default_bench_model() -> BenchModel:
    return BenchModel(
        extra_columns=(),
        indexes=(),
        run_update_columns=("payload",),
        run_extensions=(),
    )


def _load_yaml(path: str) -> dict:
    lp = path.lower()
    if not lp.endswith((".yaml", ".yml")):
        raise SystemExit(f"Model file must end with .yaml or .yml: {path!r}")
    try:
        import yaml
    except ImportError as e:
        raise SystemExit("PyYAML is required for --model: pip install pyyaml") from e
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise SystemExit("Model root must be a YAML mapping (dictionary)")
    return data


def _parse_extra_column(raw: dict, idx: int) -> ExtraColumn:
    if not isinstance(raw, dict):
        raise SystemExit(f"prepare.extra_columns[{idx}] must be a mapping")
    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        raise SystemExit(f"prepare.extra_columns[{idx}].name is required")
    name = name.strip()
    _check_ident(name, "column name")
    if name in _BUILTIN_COLS:
        raise SystemExit(f"prepare.extra_columns[{idx}].name must not shadow built-in column {name!r}")

    typ = raw.get("type")
    if not isinstance(typ, str) or not typ.strip():
        raise SystemExit(f"prepare.extra_columns[{idx}].type is required")
    typ = typ.strip().lower()

    not_null = bool(raw.get("not_null", False))
    default = raw.get("default", None)

    varchar_length = 128
    if typ == "int" or typ == "bigint" or typ == "text":
        pass
    elif typ == "varchar":
        n = raw.get("length", 128)
        if not isinstance(n, int) or n < 1 or n > 65535:
            raise SystemExit(
                f"prepare.extra_columns[{idx}].length must be an integer in [1, 65535] for type varchar"
            )
        varchar_length = n
    else:
        raise SystemExit(
            f"prepare.extra_columns[{idx}].type: unsupported {typ!r} "
            "(use int, bigint, text, varchar)"
        )

    if not_null:
        if default is None:
            raise SystemExit(
                f"prepare.extra_columns[{idx}]: not_null requires `default` "
                "(run/insert only sets payload; other columns need a default)"
            )
        if typ in ("int", "bigint"):
            if not isinstance(default, int):
                raise SystemExit(f"prepare.extra_columns[{idx}].default must be an integer")
        elif typ in ("text", "varchar"):
            if not isinstance(default, str):
                raise SystemExit(f"prepare.extra_columns[{idx}].default must be a string")

    return ExtraColumn(
        name=name,
        sql_kind=typ,
        varchar_length=varchar_length,
        not_null=not_null,
        default=default if not_null else None,
    )


def _parse_index(raw: object, idx: int) -> IndexSpec:
    if not isinstance(raw, dict):
        raise SystemExit(f"prepare.indexes[{idx}] must be a mapping")
    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        raise SystemExit(f"prepare.indexes[{idx}].name is required")
    name = name.strip()
    _check_ident(name, "index name")
    cols = raw.get("columns")
    if not isinstance(cols, list) or not cols:
        raise SystemExit(f"prepare.indexes[{idx}].columns must be a non-empty list")
    out: List[str] = []
    for j, c in enumerate(cols):
        if not isinstance(c, str) or not c.strip():
            raise SystemExit(f"prepare.indexes[{idx}].columns[{j}] must be a non-empty string")
        c = c.strip()
        _check_ident(c, "indexed column")
        out.append(c)
    unique = bool(raw.get("unique", False))
    return IndexSpec(name=name, columns=tuple(out), unique=unique)


def _parse_run_section(
    run_raw: object,
    extras: Tuple[ExtraColumn, ...],
) -> Tuple[Tuple[str, ...], Tuple[Tuple[str, Any], ...]]:
    """Parse `run` block; unknown keys are preserved in run_extensions."""
    if run_raw is None:
        return (("payload",), ())
    if not isinstance(run_raw, dict):
        raise SystemExit("run: must be a mapping when present")

    known = frozenset({"update_columns"})
    ext_items: List[Tuple[str, Any]] = []
    for k, v in run_raw.items():
        if str(k) not in known:
            ext_items.append((str(k), v))

    uc = run_raw.get("update_columns")
    if uc is None:
        run_cols = ("payload",)
    elif not isinstance(uc, list) or len(uc) == 0:
        raise SystemExit("run.update_columns: must be a non-empty list when set")
    else:
        run_cols_list: List[str] = []
        seen_u: set[str] = set()
        for j, c in enumerate(uc):
            if not isinstance(c, str) or not c.strip():
                raise SystemExit(f"run.update_columns[{j}] must be a non-empty string")
            c = c.strip()
            _check_ident(c, "update column")
            if c == "id":
                raise SystemExit("run.update_columns must not include primary key column 'id'")
            if c in seen_u:
                raise SystemExit(f"run.update_columns: duplicate column {c!r}")
            seen_u.add(c)
            run_cols_list.append(c)
        run_cols = tuple(run_cols_list)

    updatable = frozenset({"payload", "created_at"}) | {e.name for e in extras}
    for c in run_cols:
        if c not in updatable:
            raise SystemExit(
                f"run.update_columns: unknown column {c!r} "
                f"(allowed: {', '.join(sorted(updatable))})"
            )

    return (run_cols, tuple(ext_items))


def load_bench_model(path: str) -> BenchModel:
    data = _load_yaml(path)
    prep = data.get("prepare")
    if prep is None:
        prep = {}
    if not isinstance(prep, dict):
        raise SystemExit("prepare: must be a mapping when present")

    extras_raw = prep.get("extra_columns") or []
    if not isinstance(extras_raw, list):
        raise SystemExit("prepare.extra_columns must be a list")
    extras = tuple(_parse_extra_column(x, i) for i, x in enumerate(extras_raw))

    indexes_raw = prep.get("indexes") or []
    if not isinstance(indexes_raw, list):
        raise SystemExit("prepare.indexes must be a list")
    indexes = tuple(_parse_index(x, i) for i, x in enumerate(indexes_raw))

    seen: Dict[str, str] = {}
    for e in extras:
        if e.name in seen:
            raise SystemExit(f"Duplicate extra column name: {e.name!r}")
        seen[e.name] = "extra"

    allowed: FrozenSet[str] = _BUILTIN_COLS | {e.name for e in extras}
    for ix in indexes:
        for c in ix.columns:
            if c not in allowed:
                raise SystemExit(
                    f"Index {ix.name!r} references unknown column {c!r} "
                    f"(allowed: {', '.join(sorted(allowed))})"
                )

    seen_idx: Dict[str, str] = {}
    for ix in indexes:
        if ix.name in seen_idx:
            raise SystemExit(f"Duplicate index name in model: {ix.name!r}")
        seen_idx[ix.name] = "index"

    run_raw = data.get("run")
    run_update_columns, run_extensions = _parse_run_section(run_raw, extras)

    return BenchModel(
        extra_columns=extras,
        indexes=indexes,
        run_update_columns=run_update_columns,
        run_extensions=run_extensions,
    )


def physical_index_name(table: str, logical: str) -> str:
    """PostgreSQL requires index names unique in the schema; prefix with table."""
    _check_ident(table, "table name")
    _check_ident(logical, "index name")
    s = f"{table}_{logical}"
    if len(s) > _MAX_IDENT_LEN:
        raise SystemExit(
            f"Generated index name too long ({len(s)} > {_MAX_IDENT_LEN}): "
            f"shorten table stem or index name (got {s!r})"
        )
    return s
