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

2. **跑压测**（不改表结构）：

```bash
python3 db_bench.py run \
  --url 'postgresql://USER:PASS@HOST:5432/DATABASE' \
  --workers 16 \
  --duration 30 \
  --select-ratio 0.7 \
  --insert-ratio 0.3
```

多表 + 与 prepare 一致的 `table` / `tables` / `table-size`：

```bash
python3 db_bench.py run \
  --url 'postgresql://USER:PASS@HOST:5432/DATABASE' \
  --table sbtest \
  --tables 8 \
  --table-size 10000 \
  --workers 16 \
  --duration 60 \
  --mode oltp \
  --select-ratio 0.65 \
  --insert-ratio 0.25 \
  --update-ratio 0.08 \
  --delete-ratio 0.02
```

只读 / 只写示例：

```bash
python3 db_bench.py run --url 'postgresql://...' --mode read --duration 30
python3 db_bench.py run --url 'postgresql://...' --mode write --duration 30
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

### 配置文件（推荐用于固定场景）

- 使用 **`-c` / `--config`** 指定 **`.yaml` 或 `.yml`**（仅支持 YAML，便于注释与阅读）；文件中键名与下表「配置键」一致（**蛇形命名** `table_size`；也接受 **`table-size` 这种连字符写法**，会归一成 `table_size`）。
- **命令行参数会覆盖** 文件中同名项（适合把密码只放在 CLI：`--config base.yaml --url 'postgresql://…'`）。
- 示例：**[`config.example.yaml`](config.example.yaml)**。

```bash
python3 db_bench.py -c mybench.yaml prepare
python3 db_bench.py -c mybench.yaml run
python3 db_bench.py -c mybench.yaml run --duration 120   # 覆盖文件里的 duration
python3 db_bench.py -c mybench.yaml cleanup
```

文件中不认识的键会 **警告并忽略**（便于发现拼写错误）。

### Model YAML（可选）

- 使用 **`prepare --model`** / **`run --model`** 指向 YAML，可定义额外列、索引，以及 **`run.update_columns`**（压测 UPDATE 要改哪些列）。详见 **`models/example.yaml`** 与 **`models/README.md`**。
- 主配置里可设置 **`model: "models/example.yaml"`**（与 CLI 一致）。

## 压测在做什么

每个并发线程在持续时间内循环执行；**先在多张物理表上均匀随机选一张表**，再按 **增删改查** 权重随机一种操作：

- **查 SELECT（表内已有主键上界时）**：`SELECT … FROM <表> WHERE id = ?`，`id` 在 `1..上界` 内均匀随机（类似 sysbench 点查主键）。上界来自 **`prepare` 预置行数**（`--table-size`），以及**本线程**成功插入后观测到的最大 `id`。
- **查（表仍空、上界为 0）**：`SELECT … ORDER BY id DESC LIMIT 1`。
- **增 INSERT**：插入一行新 `payload`；PostgreSQL 用 `RETURNING id`、MySQL 用 `lastrowid` 更新本线程上界。
- **改 UPDATE**：按 `--model` 中 `run.update_columns` 更新列（未指定 model 时仅更新 `payload`）；`id` 的选取方式与点查相同；表空时用 `WHERE id = (SELECT MAX(id) FROM 表)`，可能更新 0 行。
- **删 DELETE**：`DELETE … WHERE id = ?`（同上；表空时用 `WHERE id = (SELECT MAX(id) FROM 表)`）。

**比例参数**（仅 **`--mode oltp`** 使用）：`--select-ratio`、`--insert-ratio`、`--update-ratio`、`--delete-ratio` 为**权重**，程序会按四者之和归一化后，作为每次循环选操作类型的概率（例如 `2 2 0 0` 与 `0.5 0.5 0 0` 等价）。默认与旧版「读写各半」一致：`0.5 / 0.5 / 0 / 0`。

通过 **`--mode`** 选择快捷预设（汇总里会打印归一化后的 CRUD 比例）：

| `--mode` | 含义 |
|----------|------|
| `oltp` | 使用上述四个 `--*-ratio` 权重（归一化后随机选操作） |
| `read` | **只查**：`select=1`，其余为 0（忽略四个 ratio） |
| `write` | **只增**：`insert=1`，其余为 0（仍为插入，不是 UPDATE/DELETE） |

在 `read` / `write` 模式下四个 ratio **不参与**最终比例。

若表刚建好尚无数据，读走 `LIMIT 1`；若希望稳定主键点查，请先 **`prepare` + `--table-size > 0`** 或先写入再测读。

### 事务模式（`--txn-mode`）

| 值 | 行为 |
|----|------|
| **`single`**（默认） | 每次循环 **一条 SQL + 一次提交**：PostgreSQL 在每条语句后 `COMMIT`；MySQL 使用连接默认 **`autocommit=true`**，每条语句自动提交。外层 CRUD 比例见上一节的 `--select-ratio` 等。 |
| **`multi`** | 每次循环 **一个多语句事务**：随机选一张表；若该表当前上界 ≥1，在事务开始时固定一个 **`txn_rid`**，事务内凡按主键 **SELECT/UPDATE/DELETE** 时优先使用该 id（类似 sysbench 在同一事务内多次触碰同一行）；表空时 `txn_rid` 为空，仍走 `LIMIT 1` / `MAX(id)` 等回退路径。事务内共执行 **`--txn-statements` 条** SQL，每条语句的类型由 **`--txn-select-ratio`、`--txn-insert-ratio`、`--txn-update-ratio`、`--txn-delete-ratio`** 四个权重归一化后**独立随机**抽取。默认 **5 / 1 / 1 / 1**（读多写少，参考 sysbench 常见 OLTP 读占比思路），默认 **`--txn-statements 7`**。`--mode read` / `write` 会同样覆盖事务内比例（全查 / 全插）。 |

**指标含义**：成功一次计 **1 笔事务**（`single` 下每次 `COMMIT`/自提交视为 1 笔；`multi` 下整段多语句 `COMMIT` 为 1 笔）。汇总与周期行里的 **TPS** = 每秒成功事务数。`multi` 时不是「单条 SQL」计数；延迟为 **整段事务** 墙钟时间。任一条语句失败则 `ROLLBACK`，记 **1 次** `errors`。

### 表数量与预置行数（参考 sysbench）

| 场景 | 行为 |
|------|------|
| `--tables 1` | 物理表名就是 `--table`（默认 `db_bench_load`） |
| `--tables N`（N>1） | 物理表名为 **`stem` + 序号**，与 sysbench 一致，例如 `--table sbtest --tables 8` → `sbtest1` … `sbtest8` |
| `--table-size 0`（默认） | `prepare` 只建表（及索引），不灌数 |
| `--table-size M`（M>0） | `prepare` 时若该表 **当前行数为 0**，则每张表预插入 **M** 行；若表已有数据则 **跳过灌数**（避免重复 `prepare` 把数据翻倍） |

PostgreSQL 预置数据使用单条 `INSERT … SELECT … generate_series`；MySQL 使用分批 `executemany`。表名、stem 仅允许 ASCII 字母、数字、下划线，且必须以字母或下划线开头，长度不超过 60。

## 参数说明（必选、默认值、配置键）

CLI 形式为 **`python3 db_bench.py <子命令> [选项]`**，子命令为 **`prepare`** | **`run`** | **`cleanup`**。下列「CLI」列对应 **`run`** 下的选项（`prepare` / `cleanup` 仅有 `--url`、`--table`、`--tables`，且 `cleanup` 无 `--table-size`；二者另有 **`--model`** 时见 `prepare`/`run` 帮助）。

**必选（二选一即可）**

| 配置键 | CLI | 说明 |
|--------|-----|------|
| `url` | `--url` | 数据库连接 URL；必须在 **配置文件** 或 **命令行** 至少一处给出非空字符串。 |

**可选（以下为程序内置默认值；未写配置文件或未在 CLI 指定时使用）**

| 配置键 | CLI | 默认值 | 说明 |
|--------|-----|--------|------|
| `workers` | `--workers` | `8` | 并发线程数（**仅 `run`**） |
| `duration` | `--duration` | `30` | 压测时长（秒） |
| `mode` | `--mode` | `oltp` | `oltp` / `read` / `write` |
| `select_ratio` | `--select-ratio` | `0.5` | 外层（`--txn-mode single`）CRUD 权重；`oltp` 下与下列三项一起归一化 |
| `insert_ratio` | `--insert-ratio` | `0.5` | 同上 |
| `update_ratio` | `--update-ratio` | `0` | 同上 |
| `delete_ratio` | `--delete-ratio` | `0` | 同上 |
| `txn_mode` | `--txn-mode` | `single` | `single` / `multi` |
| `txn_statements` | `--txn-statements` | `7` | `multi` 时每事务 SQL 条数 |
| `txn_select_ratio` | `--txn-select-ratio` | `5` | 事务内语句类型权重（`oltp` 下归一化）；与下三项默认合为 **5:1:1:1** |
| `txn_insert_ratio` | `--txn-insert-ratio` | `1` | 同上 |
| `txn_update_ratio` | `--txn-update-ratio` | `1` | 同上 |
| `txn_delete_ratio` | `--txn-delete-ratio` | `1` | 同上 |
| `table` | `--table` | `db_bench_load` | 单表全名或 stem |
| `tables` | `--tables` | `1` | 表数量 |
| `table_size` | `--table-size` | `0` | `prepare`：空表预灌行数；`run`：主键上界初始提示（应与 prepare 一致）；`0` 表示 prepare 不灌数 |
| `warmup` | `--warmup` | `0` | 预热秒数（**仅 `run`**） |
| `report_interval` | `--report-interval` | `0` | 周期打印间隔秒数；`0` 关闭 |
| `report_percentile` | `--report-percentile` | `95` | 周期行里 `lat (ms,P%)` 的分位 P（与 sysbench 默认 95 对齐） |
| `model` | `--model` | （无） | Model YAML 路径；`prepare` 与 `run` 共用（见 `models/example.yaml`） |

约束与提示：

- `report_interval` 若大于 `0` 则必须 **≥ 0.1**。
- `report_percentile` 必须在 **(0, 100]**。
- `tables` ≥ 1，`table_size` ≥ 0；`txn_mode=multi` 时 `txn_statements` ≥ 1。
- 周期报表格式见「输出说明」；极高吞吐时单窗延迟样本最多约 **10 万** 条。

### 连接 URL 格式

- PostgreSQL：`postgresql://用户:密码@主机:端口/库名`  
  也支持 `postgres://`。
- MySQL：`mysql://用户:密码@主机:端口/库名`

密码中含 `@`、`:`、`#` 等特殊字符时，请做 **URL 编码**（例如 `@` → `%40`），避免解析错误。

未写端口时：PostgreSQL 默认 `5432`，MySQL 默认 `3306`。

## 输出说明

### 压测进行中（与 sysbench 对齐，不含线程数）

若设置了 `--report-interval`，在**正式计时阶段**每隔约 N 秒打印一行，格式与 sysbench 周期统计类似（**不打印 `thds`**）：

```text
[ 5s ] tps: 2486.45 qps: 12000.12 (r/w/o: 8000.00/3000.00/1000.12) lat (ms,95%): 0.89 err/s: 0.00 reconn/s: 0.00
```

| 字段 | 含义 |
|------|------|
| `[ Ns ]` | 自**本段测量开始**起的累计秒数（整数） |
| `tps` | 该时间窗内**成功事务数 / 窗长**（秒） |
| `qps` | 该窗内**成功执行的 SQL 条数 / 窗长**（`single` 下通常 ≈ 每事务 1 条；`multi` 下为多语句之和） |
| `(r/w/o)` | **读 / 写 / 其它** 语句的 **qps**：**r**=SELECT；**w**=INSERT+UPDATE；**o**=DELETE（与 sysbench 三路拆分习惯一致） |
| `lat (ms,P%)` | 该窗内**事务耗时**的 P 分位延迟（默认 P=`--report-percentile`，默认 95） |
| `err/s` | 该窗内 **errors / 窗长** |
| `reconn/s` | 该窗内 **重连次数 / 窗长**（连接断开后再次连上时计数） |

### 压测结束（汇总）

程序结束时会在标准输出打印类似字段（整段压测的汇总）：

- `duration_s`：用于计算吞吐的统计窗口（秒）
- `transactions` / `errors`：成功事务数与错误次数（**`single`**：每条 SQL 一次提交算 1 笔；**`multi`**：每个多语句事务一次提交算 1 笔；失败则记 error）
- `throughput_tps`：每秒成功事务数（**TPS**）
- `qps` 与 `(r/w/o)`、`err/s`、`reconn/s`：整段统计窗口上的平均速率（定义与上文周期行相同：`qps` 为成功 SQL 条数/秒；**r**=SELECT/s，**w**=(INSERT+UPDATE)/s，**o**=DELETE/s）
- `latency_ms`：`min`、`max`、`mean`
- `latency_ms_p50` / `p95` / `p99`：延迟分位数（毫秒）

## 注意事项

- **断连与重连**：在 `--duration` 整段时间内，工作线程**不会因数据库短暂不可用而退出**。若发生重启、HA 切换、网络闪断等，单次请求会记一次 `errors`，连接会被丢弃并在后台重试建连；宕机期间 **TPS** 会接近 0，恢复后会继续压测直至计时结束。无法建连时使用指数退避（上限约 1 秒/次）以降低空转。新建连接使用有限超时（如 PostgreSQL `connect_timeout=10` 秒；MySQL 另设读写 socket 超时），避免在故障时无限阻塞。
- **权限**：`prepare` 需要建表、建索引权限；`cleanup` 需要删表权限；`run` 需要对应表的 `SELECT`、`INSERT` 等（PostgreSQL 写后由客户端 `COMMIT`）。
- **安全**：连接串中的密码会出现在 shell 历史或进程列表中；生产环境建议使用环境变量包装脚本或受限账号，避免使用高权限账户。
- **结果解读**：吞吐与延迟受网络、连接池、磁盘、实例规格及并发度共同影响；对比不同数据库或配置时，请固定同一套 **配置文件** 或 CLI 参数（`workers`、`duration`、`txn_mode`、CRUD / 事务内 CRUD、`tables`、`table_size` 等）。

## 许可证

若你需要为仓库选择许可证，可自行补充 `LICENSE` 文件。
