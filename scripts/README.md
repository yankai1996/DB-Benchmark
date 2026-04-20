# Sysbench Lua Scripts

This directory contains standalone sysbench Lua scripts and does not depend on
the root project README workflow.

## Script

- `sysbench_write.lua`: write workload skeleton for **MySQL/MariaDB** or **PostgreSQL** (same script; set `--db-driver=mysql` or `--db-driver=pgsql`):
  - `prepare` / `run` / `cleanup`
  - schema: `id` + `col1..colN` (`N` from `column_count`)
  - column types alternate from `col1`: `INT`, `CHAR(20)`, `INT`, `CHAR(20)`...
  - default workload mode: `batch_update_by_pk`
  - each event runs `rows_per_update` updates with random, non-repeated PKs
  - in `prepare`, `CHAR(20)` columns use a fixed value for faster data loading
- `sysbench_txn_fixed.lua`: fixed transaction script for **MySQL/MariaDB** or **PostgreSQL**
  - one `event` = one transaction
  - executes a fixed SQL sequence in order (`BEGIN -> SQL1..SQLN -> COMMIT`)
  - on any SQL failure it runs `ROLLBACK` and surfaces the error
  - SQL list is configured in script constant `FIXED_TXN_SQL`
  - supports `{table}` placeholder via `--table_name`

## Fixed Transaction Script Quick Start

Edit `scripts/sysbench_txn_fixed.lua` and replace `FIXED_TXN_SQL` with your real
business transaction SQL sequence.

Example run:

```bash
sysbench \
  --db-driver=mysql \
  --mysql-host=127.0.0.1 \
  --mysql-port=3306 \
  --mysql-user=root \
  --mysql-password=secret \
  --mysql-db=test \
  --threads=16 \
  --time=60 \
  scripts/sysbench_txn_fixed.lua \
  --table_name=sbtest \
  run
```

## Parameters (Lua options)

Pass options after the script path, for example `--column_count=8`.

- `table_name` (default: `sbtest`)
- `tables` (default: `1`)
- `table_size` (default: `180000000`)
- `column_count` (default: `4`)
- `select_col_count` (default: `column_count`, uses first N cols for select)
- `update_col_count` (default: `1`, uses first N cols for update)
- `insert_col_count` (default: `column_count`, uses first N cols for insert)
- `workload_mode` (default: `batch_update_by_pk`)
- `rows_per_update` (default: `500`)
- `write_weights` (default: `0,1,0`, format: `insert,update,delete`)
- `insert_batch_size` (default: `10000`, used in `prepare`)
- `prepare_mode` (default: `sql_generate`, options: `lua_values`, `sql_generate`)

When `*_col_count` is larger than `column_count`, it is clamped to `column_count`.

`prepare_mode=sql_generate` generates batch rows inside SQL (MySQL: digit cross-join + subquery; PostgreSQL: `generate_series`), which is
typically faster than Lua constructing long VALUES lists at very large scales.
In this mode, integer columns use a lightweight deterministic formula
based on row sequence (instead of per-column `RAND()` calls).

## Examples

### PostgreSQL (prepare)

```bash
sysbench \
  --db-driver=pgsql \
  --pgsql-host=127.0.0.1 \
  --pgsql-port=5432 \
  --pgsql-user=postgres \
  --pgsql-password=secret \
  --pgsql-db=test \
  scripts/sysbench_write.lua \
  --table_size=100000 \
  --column_count=8 \
  --prepare_mode=sql_generate \
  prepare
```

### 1) Prepare (MySQL)

```bash
sysbench \
  --db-driver=mysql \
  --mysql-host=127.0.0.1 \
  --mysql-port=3306 \
  --mysql-user=root \
  --mysql-password=secret \
  --mysql-db=test \
  scripts/sysbench_write.lua \
  --table_name=bench \
  --tables=4 \
  --table_size=50000 \
  --column_count=8 \
  --insert_col_count=8 \
  --prepare_mode=sql_generate \
  prepare
```

### 2) Run

```bash
sysbench \
  --db-driver=mysql \
  --mysql-host=127.0.0.1 \
  --mysql-port=3306 \
  --mysql-user=root \
  --mysql-password=secret \
  --mysql-db=test \
  --threads=16 \
  --time=60 \
  scripts/sysbench_write.lua \
  --workload_mode=batch_update_by_pk \
  --rows_per_update=20 \
  --update_col_count=6 \
  --select_col_count=4 \
  run
```

### 3) Cleanup

```bash
sysbench \
  --db-driver=mysql \
  --mysql-host=127.0.0.1 \
  --mysql-port=3306 \
  --mysql-user=root \
  --mysql-password=secret \
  --mysql-db=test \
  scripts/sysbench_write.lua \
  --table_name=bench \
  --tables=4 \
  cleanup
```

## Extension Points

- Schema extension: edit `build_create_table_sql`.
- Workload extension: add modes in `dispatch_workload`.
- Row filter extension: add new functions for range/IN/custom-where updates.
