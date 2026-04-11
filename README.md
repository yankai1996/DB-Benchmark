# DB-Benchmark

在 Linux（及 macOS）上运行的轻量级数据库压测工具：用多线程并发对 **PostgreSQL** 或 **MySQL** 发起读写混合负载，并输出吞吐与延迟分位数。

## 环境要求

- Python 3.8+
- 目标数据库可网络访问，且账号具备相应 DDL/DML 权限（首次建表时需要）

## 安装

```bash
git clone <你的仓库地址> DB-Benchmark
cd DB-Benchmark

python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

依赖说明：

- PostgreSQL：`psycopg2-binary`
- MySQL：`pymysql`
- 配置文件（YAML）：`PyYAML`（`pip install -r requirements.txt` 会安装）

## 快速开始

命令行使用 **子命令**：`prepare`（建表/灌数）→ `run`（压测）→ `cleanup`（删表）。连接串写在配置文件或 `--url`。

1. **准备环境**（首次或换表时）：创建压测表和索引；需要 **sysbench 风格多表 + 预置行数** 时用 `--table`、`--tables`、`--table-size`。

```bash
python3 db_bench.py prepare --url 'postgresql://USER:PASS@HOST:5432/DATABASE'
```

多表示例（8 张表、每表 1 万行，表名 `sbtest1` … `sbtest8`）：

```bash
python3 db_bench.py prepare \
  --url 'postgresql://USER:PASS@HOST:5432/DATABASE' \
  --table sbtest \
  --tables 8 \
  --table-size 10000
```

2. **跑压测**：负载形状由 **`--model` 指向的 YAML** 中的 **`workload`** 决定（默认 **`models/default.yaml`**）。临时改比例用 **`run` 的 CLI**。

```bash
python3 db_bench.py run \
  --url 'postgresql://USER:PASS@HOST:5432/DATABASE' \
  --workers 16 \
  --duration 30
```

多表 + 与 prepare 一致的 `table` / `tables` / `table-size`：

```bash
python3 db_bench.py run \
  --url 'postgresql://USER:PASS@HOST:5432/DATABASE' \
  --table sbtest \
  --tables 8 \
  --table-size 10000 \
  --workers 16 \
  --duration 60
```

一次性覆盖 CRUD 权重示例：

```bash
python3 db_bench.py run --url 'postgresql://...' --select-ratio 0.7 --insert-ratio 0.3 --duration 30
```

只读 / 只写（用权重表达：归一化后仅 `select` 或仅 `insert` 非零）：

```bash
python3 db_bench.py run --url 'postgresql://...' --select-ratio 1 --insert-ratio 0 --duration 30
python3 db_bench.py run --url 'postgresql://...' --select-ratio 0 --insert-ratio 1 --duration 30
```

MySQL 示例：

```bash
python3 db_bench.py prepare --url 'mysql://USER:PASS@HOST:3306/DATABASE'
python3 db_bench.py run --url 'mysql://USER:PASS@HOST:3306/DATABASE' --workers 16 --duration 30
```

3. **收尾**（删除本次使用的压测表）：

```bash
python3 db_bench.py cleanup --url 'postgresql://...' --table sbtest --tables 8
```

4. 查看帮助：

```bash
python3 db_bench.py --help
python3 db_bench.py prepare --help
python3 db_bench.py run --help
python3 db_bench.py cleanup --help
```

### 配置文件（`-c` / `--config`）

- 仅支持 **`.yaml` / `.yml`**；键名为 **蛇形**（`table_size`），也接受连字符（`table-size`），会归一成蛇形。
- **允许的键**见下表「主配置文件」；**不得**写入任何 CRUD/事务比例键（程序会当作未知键警告并忽略）。
- **命令行**覆盖文件中同名项（例如只把密码放在 CLI：`--config base.yaml --url 'postgresql://…'`）。
- 示例：**[`config.example.yaml`](config.example.yaml)**。

```bash
python3 db_bench.py -c mybench.yaml prepare
python3 db_bench.py -c mybench.yaml run
python3 db_bench.py -c mybench.yaml run --duration 120
python3 db_bench.py -c mybench.yaml cleanup
```

不认识的键会 **警告并忽略**。

### Model YAML

- 通过 **`--model`** 或主配置里的 **`model:`** 指定；默认 **`models/default.yaml`**。
- **`prepare.*`**：额外列、索引；**`run.update_columns`**：UPDATE 涉及的列。
- **`workload`**：**Mix 轨**（**`mix.single`** / **`mix.multi`**，与 **`transaction.shape`** 一致）或 **模板轨**（非空 **`statements`**，每项 **`id`、`weight`、`kind`**，可选 **`sql`**）。两轨 **互斥**。若省略 **`workload:`**，解析结果与内置 **`default_workload_spec`** 相同。**`run`** 的 **CLI 比例参数**可覆盖该次运行使用的数值。
- **模板轨自定义 `sql`**（省略 `sql` 则仍用内置语句）：
  - 占位符：**`{table}`** 替换为当前随机到的物理表名；**`{select_list}`** 替换为与内置 SELECT 相同的列列表（`id` 与 **`run.update_columns`**）。
  - 参数占位符为 **DB-API 风格 `%s`**（字面 `%` 请写成 `%%`）。条数规则：**`select`**：`0`、`1`（点查 `id`）或 **`2`（范围查，如 `BETWEEN %s AND %s`）**；**`delete`**：`0` 或 `1`；**`insert`**：`0` 或 `1`（`payload`）；**`update`**：`0`、`1` 或 `2`（`payload`、`id`）。范围查时程序在 **`[1, 当前表上界]`** 内随机一段长度 **`range_size`** 的闭区间（上界不足时缩短为 **`[1, top]`**）；**`workload.range_size`**（默认 `100`，与常见 sysbench 一致）可被 **`run --range-size`** 或单条 **`statements[].range_size`** 覆盖。无行时范围查会报错计入 **`errors`**。点查无上界时同上。
  - **INSERT** 后程序会尝试用 **`lastrowid`**（MySQL）或 **`RETURNING` 首列**（PostgreSQL：请在 SQL 中写 **`RETURNING id`** 以便更新线程内主键上界）刷新上界。
- 详见 **`models/example.yaml`**、**`models/default.yaml`**、**`models/template_track_example.yaml`**、**[`models/template_custom_sql_example.yaml`](models/template_custom_sql_example.yaml)**、**`models/README.md`**。

## 压测在做什么

每个并发线程在持续时间内循环执行；**先在多张物理表上均匀随机选一张表**，再按 **增删改查** 权重随机一种操作：

- **查 SELECT（表内已有主键上界时）**：`SELECT … FROM <表> WHERE id = ?`，`id` 在 `1..上界` 内均匀随机（类似 sysbench 点查主键）。上界来自 **`prepare` 预置行数**（`--table-size`），以及**本线程**成功插入后观测到的最大 `id`。
- **查（表仍空、上界为 0）**：`SELECT … ORDER BY id DESC LIMIT 1`。
- **增 INSERT**：插入一行新 `payload`；PostgreSQL 用 `RETURNING id`、MySQL 用 `lastrowid` 更新本线程上界。
- **改 UPDATE**：按当前 model 的 **`run.update_columns`** 更新列（默认仅 `payload`）；`id` 的选取方式与点查相同；表空时用 `WHERE id = (SELECT MAX(id) FROM 表)`，可能更新 0 行。
- **删 DELETE**：`DELETE … WHERE id = ?`（同上；表空时用 `WHERE id = (SELECT MAX(id) FROM 表)`）。

**比例**：`--select-ratio` 等为**权重**，四者之和归一化后抽样。**基准值**来自当前 **`--model`** 解析出的 **`workload`**；**`run` 的 CLI** 传入则覆盖。归一化后仅 SELECT 非零为只读；仅 INSERT 非零为只写；否则为 OLTP。

若表刚建好尚无数据，读走 `LIMIT 1`；若希望稳定主键点查，请先 **`prepare` + `--table-size > 0`** 或先写入再测读。

### 事务模式（`--txn-mode`）

| 值 | 行为 |
|----|------|
| **`single`** | 每次循环 **一条 SQL + 一次提交**：PostgreSQL 每条语句后 `COMMIT`；MySQL 默认 **`autocommit=true`**。外层 CRUD 比例见 **`workload.mix.single`**（可被 CLI 覆盖）。 |
| **`multi`** | 每次循环 **一个多语句事务**：随机选表；若上界 ≥1，事务内固定 **`txn_rid`** 用于主键类语句。事务内共 **`--txn-statements` 条**（或 model **`workload.transaction.statement_count`**），类型由 **事务内权重**（**`workload.mix.multi`** 或 CLI **`--txn-*-ratio`**）独立抽样。外层若归一化为只读/只写，事务内比例也会变为全查/全插。 |

**指标**：成功一次计 **1 笔事务**（`multi` 下整段 `COMMIT` 为 1 笔）。**TPS** = 每秒成功事务数。**延迟**为整段事务墙钟时间（`multi` 为多语句之和）。

### 表数量与预置行数（参考 sysbench）

| 场景 | 行为 |
|------|------|
| `--tables 1` | 物理表名就是 `--table`（默认 `db_bench_load`） |
| `--tables N`（N>1） | 物理表名为 **`stem` + 序号**，例如 `--table sbtest --tables 8` → `sbtest1` … `sbtest8` |
| `--table-size 0`（默认） | `prepare` 只建表（及索引），不灌数 |
| `--table-size M`（M>0） | `prepare` 时若该表 **当前行数为 0**，则每张表预插入 **M** 行；已有数据则跳过灌数 |

PostgreSQL 预置数据使用单条 `INSERT … SELECT … generate_series`；MySQL 使用分批 `executemany`。表名、stem 仅允许 ASCII 字母、数字、下划线，且必须以字母或下划线开头，长度不超过 60。

## 参数说明

CLI 形式：**`python3 db_bench.py <子命令> [选项]`**，子命令为 **`prepare`** | **`run`** | **`cleanup`**。下表「主配置文件」与「必选 `url`」适用于带 **`-c`** 的场景；**`prepare`** / **`cleanup`** 仅支持 **`--url`**、**`--table`**、**`--tables`**（**`prepare`** 另有 **`--table-size`**、**`--model`**；**`cleanup`** 无 **`--table-size`**），其余见各子命令 **`--help`**。

**必选（二选一）**

| 配置键（仅 `-c`） | CLI | 说明 |
|-------------------|-----|------|
| `url` | `--url` | 数据库 URL；须在配置文件或命令行至少一处非空。 |

**主配置文件（`-c` YAML）可选键**

| 配置键 | CLI | 默认值 | 说明 |
|--------|-----|--------|------|
| `workers` | `--workers` | `8` | 并发线程数（**仅 `run`**） |
| `duration` | `--duration` | `30` | 压测时长（秒） |
| `table` | `--table` | `db_bench_load` | 单表全名或 stem |
| `tables` | `--tables` | `1` | 表数量 |
| `table_size` | `--table-size` | `0` | `prepare`：空表预灌行数；`run`：主键上界提示 |
| `warmup` | `--warmup` | `0` | 预热秒数（**仅 `run`**） |
| `report_interval` | `--report-interval` | `0` | 周期打印间隔；`0` 关闭 |
| `report_percentile` | `--report-percentile` | `95` | 周期行延迟分位 P |
| `model` | `--model` | `models/default.yaml`（与程序同目录） | Model 路径 |

**仅 `run` 子命令 — 负载与事务（CLI；不可写入主 YAML）**

默认值随 **`--model`** 中解析的 **`workload`** 变化；`python3 db_bench.py run --help` 显示当前合并后的默认数。下表为 **内置 `default_workload_spec`** 对应的示意。

| CLI | 示意默认 | 说明 |
|-----|----------|------|
| `--select-ratio` 等 | `0.5` / `0.5` / `0` / `0` | 外层 CRUD 权重（`single` 下 **mix.single**） |
| `--txn-mode` | `single` | `single` / `multi` |
| `--txn-statements` | `7` | `multi` 时每事务 SQL 条数 |
| `--txn-select-ratio` 等 | `5` / `1` / `1` / `1` | 事务内权重（`multi` 下 **mix.multi**） |
| `--range-size` | `100` | 模板 `SELECT` 两个 `%s`（`BETWEEN`）时的区间长度；可由 **`workload.range_size`** 合并为默认 |

约束：`report_interval` > 0 时须 **≥ 0.1**；`report_percentile` ∈ **(0, 100]**；`tables` ≥ 1；`txn_mode=multi` 时 `txn_statements` ≥ 1；**`--range-size`** ≥ 1。

### 连接 URL 格式

- PostgreSQL：`postgresql://用户:密码@主机:端口/库名`（也支持 `postgres://`）
- MySQL：`mysql://用户:密码@主机:端口/库名`

密码中含 `@`、`:`、`#` 等时请 **URL 编码**（如 `@` → `%40`）。未写端口时：PostgreSQL `5432`，MySQL `3306`。

## 输出说明

### 压测进行中（与 sysbench 对齐，不含线程数）

若设置了 `--report-interval`，在**正式计时阶段**每隔约 N 秒打印一行：

```text
[ 5s ] tps: 2486.45 qps: 12000.12 (r/w/o: 8000.00/3000.00/1000.12) lat (ms,95%): 0.89 err/s: 0.00 reconn/s: 0.00
```

| 字段 | 含义 |
|------|------|
| `[ Ns ]` | 自本段测量起的累计秒数 |
| `tps` | 该窗内成功事务数 / 窗长 |
| `qps` | 该窗内成功 SQL 条数 / 窗长 |
| `(r/w/o)` | 读 / 写(INSERT+UPDATE) / 其它(DELETE) 的 qps |
| `lat (ms,P%)` | 该窗内事务耗时的 P 分位延迟 |
| `err/s` | 错误次数 / 窗长 |
| `reconn/s` | 重连次数 / 窗长 |

### 压测结束（汇总）

程序结束时会在标准输出打印整段压测的汇总，例如：

- `duration_s`：用于计算吞吐的统计窗口（秒）
- `transactions` / `errors`：成功事务数与错误次数（**`single`**：每条 SQL 一次提交算 1 笔；**`multi`**：每个多语句事务一次提交算 1 笔；失败则记 error）
- `throughput_tps`：每秒成功事务数（**TPS**）
- `qps` 与 `(r/w/o)`、`err/s`、`reconn/s`：整段窗口上的平均速率（**r**=SELECT/s，**w**=(INSERT+UPDATE)/s，**o**=DELETE/s）
- `latency_ms`：`min`、`max`、`mean`
- `latency_ms_p50` / `p95` / `p99`：延迟分位数（毫秒）

## 注意事项

- **断连与重连**：在 `--duration` 整段时间内，工作线程**不会因数据库短暂不可用而退出**。若发生重启、HA 切换、网络闪断等，单次请求会记一次 `errors`，连接会被丢弃并在后台重试建连；宕机期间 **TPS** 会接近 0，恢复后会继续压测直至计时结束。无法建连时使用指数退避（上限约 1 秒/次）以降低空转。新建连接使用有限超时（如 PostgreSQL `connect_timeout=10` 秒；MySQL 另设读写 socket 超时），避免在故障时无限阻塞。
- **权限**：`prepare` 需要建表、建索引权限；`cleanup` 需要删表权限；`run` 需要对应表的 `SELECT`、`INSERT` 等（PostgreSQL 写后由客户端 `COMMIT`）。
- **安全**：连接串中的密码会出现在 shell 历史或进程列表中；生产环境建议使用环境变量包装脚本或受限账号，避免使用高权限账户。
- **结果解读**：吞吐与延迟受网络、连接池、磁盘、实例规格及并发度共同影响；对比不同数据库或配置时，请固定同一套 **model**、**`-c` 主配置** 与 **`run` CLI**（`workers`、`duration`、`txn_mode`、CRUD / 事务内权重、`tables`、`table_size` 等）。

## 许可证

若你需要为仓库选择许可证，可自行补充 `LICENSE` 文件。
