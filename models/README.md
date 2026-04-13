# Model YAML 说明

Bench **model** 描述可选的额外列、索引、**`run.update_columns`**，以及 **`workload`**（**Mix 轨**：`mix.*`；**模板轨**：`statements`，与 `mix` 互斥）。通过 **`prepare --model`** / **`run --model`** 或主 YAML **`model:`** 指定；未指定时使用 **`models/default.yaml`**。主配置文件（**`-c`**）**不得**包含 CRUD/事务比例键；比例在本文件的 **`workload`**（省略时与内置 **`default_workload_spec`** 相同）中定义，**`run`** 的 CLI 可临时覆盖。

- **`default.yaml`**：内置默认（基础表 + 索引 + Mix 轨 `workload`）。
- **`example.yaml`**：示例——在默认内置列（`id,k,c,pad`）之外增加额外列、索引、**`run.update_columns`**；未写 **`workload`** 时与 **`default_workload_spec`** 相同。
- **`template_track_example.yaml`**：模板轨示例（仅 `kind` + 权重，内置 SQL）。
- **`template_custom_sql_example.yaml`**：模板轨 + 自定义 **`sql`**（点查 / **`BETWEEN` 范围查**、`{table}` / `{select_list}` / `%s`，**`workload.range_size`**）。
- **`multi_mix_example.yaml`**：**Mix 轨** + **`workload.transaction.shape: multi`**；每个事务的语句条数由 **`workload.transaction.statement_count`** 配置（勿放在主配置 `-c` YAML 里）。
- **`sequence_example.yaml`**：**固定顺序**多语句事务（**`workload.sequence`**），与 **`workload.mix`**、**`workload.statements`** 互斥；无比例、无随机槽位，仅占位符数值变化。
- **`workload_bind_spec.md`**：模板自定义 SQL 的声明式 **`bind`** 合法取值表（设计稿，落地实现时可按此校验）。

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
