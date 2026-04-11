# Model YAML 说明

Bench **model** 用于描述可选的额外列、索引，以及运行时 **UPDATE** 要改哪些列。通过 **`prepare --model`** 与 **`run --model`** 传入文件路径（或在主压测配置里设置 **`model`**）。

- **`example.yaml`**：示例——包含额外列 `k`、索引，以及 **`run.update_columns`**。

可在本目录下增加更多预设（例如 `pg_minimal.yaml`），使用时写相对或绝对路径即可。

## `ddl_to_model.py`（DDL 转 model YAML）

将 **部分** SQL DDL 转成 model YAML，用于快速起稿；**生成结果务必人工核对**。

```bash
python3 models/ddl_to_model.py -i your_table.sql -o models/generated.yaml
# 或
python3 models/ddl_to_model.py < your_table.sql
```

**能识别**：`CREATE TABLE` 里的列定义，以及单独的 **`CREATE [UNIQUE] INDEX … ON … (…)`** 语句。

**不能识别**：写在 **`CREATE TABLE` 内部的** 内联 **`INDEX (...)`**（常见于 MySQL）——请改为单独的 `CREATE INDEX` 写在 DDL 里，或在 YAML 里手写 **`prepare.indexes`**。

**类型映射**：除 `int` / `bigint` / `text` / `varchar(n)` 外，其它列类型会统一输出为 **`varchar(128)`**，并在 **stderr** 给出警告；**`SERIAL` / `BIGSERIAL` / `GENERATED`** 行仍会跳过（通常对应自增主键列）。
