"""YAML model for `prepare --model` / `run --model`: DDL, indexes, run.update_columns, workload.*."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_IDENT_LEN = 63
_COL_BIND_PREFIX_RE = re.compile(r"^col:[A-Za-z_][A-Za-z0-9_]*$")

_BUILTIN_COLS = frozenset({"id", "k", "c", "pad"})


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
class StatementTemplate:
    """Weighted statement slot for template track (builtin SQL per kind, or custom sql)."""

    id: str
    weight: float
    kind: str  # select | insert | update | delete
    sql: Optional[str]
    range_size: Optional[int]  # select + 2x %s: override workload.range_size for BETWEEN width
    bind: Tuple[str, ...]  # required when sql is set: row_id | range_pair | col:<name>


@dataclass(frozen=True)
class SequenceStep:
    """One step in workload.sequence: fixed order, no weights (multi-statement transactions only)."""

    id: str
    kind: str  # select | insert | update | delete
    sql: Optional[str]
    range_size: Optional[int]
    bind: Tuple[str, ...]


@dataclass(frozen=True)
class WorkloadSpec:
    """Transaction shape + mix weights, weighted templates, or fixed sequence (mutually exclusive)."""

    shape: str  # single | multi
    statement_count: int  # used when shape == multi (>= 1)
    mix_single: Tuple[float, float, float, float]  # select, insert, update, delete
    mix_multi: Tuple[float, float, float, float]
    statements: Tuple[StatementTemplate, ...]  # non-empty => template track
    range_size: int  # template SELECT with two %s (BETWEEN low/high); default 100 (sysbench-like)
    sequence: Tuple[SequenceStep, ...]  # non-empty => fixed-order multi track


def default_workload_spec() -> WorkloadSpec:
    return WorkloadSpec(
        shape="single",
        statement_count=7,
        mix_single=(0.5, 0.5, 0.0, 0.0),
        mix_multi=(5.0, 1.0, 1.0, 1.0),
        statements=(),
        range_size=100,
        sequence=(),
    )


@dataclass(frozen=True)
class BenchModel:
    """prepare DDL plus run workload; run_extensions stores unknown keys under `run` for future use."""

    extra_columns: Tuple[ExtraColumn, ...]
    indexes: Tuple[IndexSpec, ...]
    run_update_columns: Tuple[str, ...]
    run_extensions: Tuple[Tuple[str, Any], ...]
    workload: WorkloadSpec


def default_bench_model() -> BenchModel:
    return BenchModel(
        extra_columns=(),
        indexes=(),
        run_update_columns=("k", "c"),
        run_extensions=(),
        workload=default_workload_spec(),
    )


def _parse_mix_weights(raw: object, where: str) -> Tuple[float, float, float, float]:
    if not isinstance(raw, dict):
        raise SystemExit(f"{where}: must be a mapping")
    out: List[float] = []
    for k in ("select", "insert", "update", "delete"):
        v = raw.get(k)
        if v is None:
            raise SystemExit(f"{where}.{k} is required")
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise SystemExit(f"{where}.{k} must be a number")
        if v < 0:
            raise SystemExit(f"{where}.{k} must be >= 0")
        out.append(float(v))
    if sum(out) <= 0:
        raise SystemExit(f"{where}: at least one of select/insert/update/delete must be > 0")
    return (out[0], out[1], out[2], out[3])


def _coerce_pos_int(name: str, raw: object) -> int:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise SystemExit(f"{name} must be an integer")
    if isinstance(raw, float) and not raw.is_integer():
        raise SystemExit(f"{name} must be an integer")
    n = int(raw)
    if n < 1:
        raise SystemExit(f"{name} must be >= 1")
    return n


def _parse_bind_list(raw: object, where: str) -> Tuple[str, ...]:
    """Declarative bind atoms: row_id, range_pair, col:<column>. See models/workload_bind_spec.md."""
    if not isinstance(raw, list):
        raise SystemExit(f"{where} must be a list")
    out: List[str] = []
    for i, x in enumerate(raw):
        if not isinstance(x, str) or not x.strip():
            raise SystemExit(f"{where}[{i}] must be a non-empty string")
        atom = x.strip()
        if atom in ("row_id", "range_pair"):
            out.append(atom)
        elif _COL_BIND_PREFIX_RE.match(atom):
            out.append(atom)
        else:
            raise SystemExit(
                f"{where}[{i}] must be 'row_id', 'range_pair', or 'col:<column>' (got {x!r})"
            )
    return tuple(out)


def _parse_workload_range_size(raw: object) -> int:
    """Default 100 when omitted (aligns with common sysbench --range-size)."""
    if raw is None:
        return 100
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise SystemExit("workload.range_size must be an integer")
    if isinstance(raw, float) and not raw.is_integer():
        raise SystemExit("workload.range_size must be an integer")
    n = int(raw)
    if n < 1:
        raise SystemExit("workload.range_size must be >= 1")
    return n


def _parse_statement_template(raw: object, idx: int) -> StatementTemplate:
    if not isinstance(raw, dict):
        raise SystemExit(f"workload.statements[{idx}] must be a mapping")
    tid = raw.get("id")
    if not isinstance(tid, str) or not tid.strip():
        raise SystemExit(f"workload.statements[{idx}].id is required")
    w = raw.get("weight")
    if isinstance(w, bool) or not isinstance(w, (int, float)):
        raise SystemExit(f"workload.statements[{idx}].weight must be a number")
    if w < 0:
        raise SystemExit(f"workload.statements[{idx}].weight must be >= 0")
    kind_raw = raw.get("kind")
    if not isinstance(kind_raw, str) or not kind_raw.strip():
        raise SystemExit(f"workload.statements[{idx}].kind is required")
    kind = kind_raw.strip().lower()
    if kind not in ("select", "insert", "update", "delete"):
        raise SystemExit(
            f"workload.statements[{idx}].kind must be select, insert, update, or delete "
            f"(got {kind_raw!r})"
        )
    sql_raw = raw.get("sql")
    sql: Optional[str]
    if sql_raw is None or sql_raw == "":
        sql = None
    elif isinstance(sql_raw, str):
        s = sql_raw.strip()
        sql = None if not s else s
    else:
        raise SystemExit(f"workload.statements[{idx}].sql must be a string or omitted")
    rs_stmt: Optional[int] = None
    if raw.get("range_size") is not None:
        rs_stmt = _coerce_pos_int(f"workload.statements[{idx}].range_size", raw.get("range_size"))
    bind_raw = raw.get("bind")
    if sql is not None:
        if bind_raw is None:
            raise SystemExit(f"workload.statements[{idx}].bind is required when sql is set")
        bind = _parse_bind_list(bind_raw, f"workload.statements[{idx}].bind")
    else:
        if bind_raw is not None:
            raise SystemExit(f"workload.statements[{idx}].bind must be omitted when sql is omitted")
        bind = ()
    return StatementTemplate(tid.strip(), float(w), kind, sql, rs_stmt, bind)


def _parse_sequence_step(raw: object, idx: int) -> SequenceStep:
    if not isinstance(raw, dict):
        raise SystemExit(f"workload.sequence[{idx}] must be a mapping")
    tid = raw.get("id")
    if not isinstance(tid, str) or not tid.strip():
        raise SystemExit(f"workload.sequence[{idx}].id is required")
    kind_raw = raw.get("kind")
    if not isinstance(kind_raw, str) or not kind_raw.strip():
        raise SystemExit(f"workload.sequence[{idx}].kind is required")
    kind = kind_raw.strip().lower()
    if kind not in ("select", "insert", "update", "delete"):
        raise SystemExit(
            f"workload.sequence[{idx}].kind must be select, insert, update, or delete "
            f"(got {kind_raw!r})"
        )
    sql_raw = raw.get("sql")
    sql: Optional[str]
    if sql_raw is None or sql_raw == "":
        sql = None
    elif isinstance(sql_raw, str):
        s = sql_raw.strip()
        sql = None if not s else s
    else:
        raise SystemExit(f"workload.sequence[{idx}].sql must be a string or omitted")
    rs_stmt: Optional[int] = None
    if raw.get("range_size") is not None:
        rs_stmt = _coerce_pos_int(f"workload.sequence[{idx}].range_size", raw.get("range_size"))
    bind_raw = raw.get("bind")
    if sql is not None:
        if bind_raw is None:
            raise SystemExit(f"workload.sequence[{idx}].bind is required when sql is set")
        bind = _parse_bind_list(bind_raw, f"workload.sequence[{idx}].bind")
    else:
        if bind_raw is not None:
            raise SystemExit(f"workload.sequence[{idx}].bind must be omitted when sql is omitted")
        bind = ()
    return SequenceStep(tid.strip(), kind, sql, rs_stmt, bind)


def parse_workload(raw: object) -> WorkloadSpec:
    """Parse optional top-level `workload` block; missing or {} uses defaults."""
    if raw is None or raw == {}:
        return default_workload_spec()
    if not isinstance(raw, dict):
        raise SystemExit("workload: must be a mapping")

    tx = raw.get("transaction")
    if not isinstance(tx, dict):
        raise SystemExit("workload.transaction is required (mapping)")

    shape_raw = tx.get("shape")
    if not isinstance(shape_raw, str) or not shape_raw.strip():
        raise SystemExit("workload.transaction.shape must be 'single' or 'multi'")
    shape = shape_raw.strip().lower()
    if shape not in ("single", "multi"):
        raise SystemExit("workload.transaction.shape must be 'single' or 'multi'")

    stmts_raw = raw.get("statements")
    has_nonempty_statements = isinstance(stmts_raw, list) and len(stmts_raw) > 0
    if stmts_raw is not None and not isinstance(stmts_raw, list):
        raise SystemExit("workload.statements must be a list")

    d = default_workload_spec()
    wrs = _parse_workload_range_size(raw.get("range_size"))

    seq_raw = raw.get("sequence")
    if seq_raw is not None:
        if not isinstance(seq_raw, list):
            raise SystemExit("workload.sequence must be a list")
        if len(seq_raw) < 1:
            raise SystemExit("workload.sequence must be non-empty")
        if has_nonempty_statements:
            raise SystemExit("workload.statements must be empty when workload.sequence is set")
        if "mix" in raw:
            raise SystemExit("workload.mix must not appear when workload.sequence is non-empty")
        if shape != "multi":
            raise SystemExit("workload.transaction.shape must be multi when workload.sequence is set")
        steps = tuple(_parse_sequence_step(x, i) for i, x in enumerate(seq_raw))
        n = len(steps)
        sc = tx.get("statement_count")
        if sc is None:
            n_st = n
        else:
            n_st = _coerce_pos_int("workload.transaction.statement_count", sc)
            if n_st != n:
                raise SystemExit(
                    f"workload.transaction.statement_count ({n_st}) must equal "
                    f"len(workload.sequence) ({n})"
                )
        return WorkloadSpec("multi", n_st, d.mix_single, d.mix_multi, (), wrs, steps)

    if has_nonempty_statements:
        if "mix" in raw:
            raise SystemExit(
                "workload.mix must not appear when workload.statements is non-empty "
                "(use mix track or template track, not both)"
            )
        templates = tuple(_parse_statement_template(x, i) for i, x in enumerate(stmts_raw))
        tw = sum(t.weight for t in templates)
        if tw <= 0:
            raise SystemExit("workload.statements: sum of weights must be > 0")
        if shape == "single":
            if "statement_count" in tx:
                raise SystemExit("workload.transaction.statement_count is not allowed when shape is single")
            return WorkloadSpec("single", d.statement_count, d.mix_single, d.mix_multi, templates, wrs, ())
        n_st = _coerce_pos_int("workload.transaction.statement_count", tx.get("statement_count"))
        return WorkloadSpec("multi", n_st, d.mix_single, d.mix_multi, templates, wrs, ())

    mix = raw.get("mix")
    if not isinstance(mix, dict):
        raise SystemExit("workload.mix is required when workload.statements is absent or empty")

    if shape == "single":
        if "multi" in mix:
            raise SystemExit("workload.mix.multi is not allowed when transaction.shape is single")
        if "single" not in mix:
            raise SystemExit("workload.mix.single is required when transaction.shape is single")
        ms = _parse_mix_weights(mix["single"], "workload.mix.single")
        if "statement_count" in tx:
            raise SystemExit("workload.transaction.statement_count is not allowed when shape is single")
        return WorkloadSpec("single", d.statement_count, ms, d.mix_multi, (), wrs, ())

    if "single" in mix:
        raise SystemExit("workload.mix.single is not allowed when transaction.shape is multi")
    if "multi" not in mix:
        raise SystemExit("workload.mix.multi is required when transaction.shape is multi")
    mm = _parse_mix_weights(mix["multi"], "workload.mix.multi")
    n_st = _coerce_pos_int("workload.transaction.statement_count", tx.get("statement_count"))
    return WorkloadSpec("multi", n_st, d.mix_single, mm, (), wrs, ())


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
                    "(run/insert only sets built-in columns and explicitly updated extras; "
                    "other columns need a default)"
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
        return (("k", "c"), ())
    if not isinstance(run_raw, dict):
        raise SystemExit("run: must be a mapping when present")

    if run_raw.get("mode") is not None:
        raise SystemExit(
            "run.mode is removed; set workload in the model YAML (workload.mix / workload.statements) "
            "and use run CLI flags to override ratios when needed."
        )

    known = frozenset({"update_columns"})
    ext_items: List[Tuple[str, Any]] = []
    for k, v in run_raw.items():
        if str(k) not in known:
            ext_items.append((str(k), v))

    uc = run_raw.get("update_columns")
    if uc is None:
        run_cols = ("k", "c")
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

    updatable = frozenset({"k", "c", "pad"}) | {e.name for e in extras}
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

    workload = parse_workload(data.get("workload"))

    return BenchModel(
        extra_columns=extras,
        indexes=indexes,
        run_update_columns=run_update_columns,
        run_extensions=run_extensions,
        workload=workload,
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
