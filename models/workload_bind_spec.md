# Workload 模板 `bind` 合法取值表（设计稿）

本文定义 **`workload.statements[]` / `workload.sequence[]`** 在提供自定义 **`sql`** 时，字段 **`bind`** 的语义与合法取值。  
实现时可按此校验：**展开后的 SQL 中 `%s` 的个数** 必须等于 **`bind` 展开后占用的占位符个数之和**。

---

## 1. 总则

- **`bind`**：YAML 数组，元素为 **绑定原子**（见 §2）。按 **从左到右** 的顺序，与 SQL 中 **`%s` 从左到右** 一一对应（宏替换 **`{table}`、`{select_list}`** 在数 `%s` 之前完成）。
- **不向后兼容**：有自定义 `sql` 时假定 **必须写 `bind`**（除非实现另行说明）。
- **占位符**：仅支持 DB-API 风格 **`%s`**；`%%` 视为字面量 `%`，不计入占位符。
- **多语句事务**：`row_id` / `range_pair` 使用 **表行上界 `pk_hi[table]`** 与 **`fixed_rid`**；若上界为 0 且需要 `row_id`，执行失败。

---

## 2. 绑定原子

每个原子占用 **1 个或 2 个** `%s`（**arity**）。

### 2.1 与列无关

| 原子 id | arity | 生成的值 | 说明 |
|---------|-------|----------|------|
| **`row_id`** | 1 | 主键 `id` | 优先 `fixed_rid`；否则在 `[1, pk_hi[tbl]]` 上均匀随机；`pk_hi[tbl] < 1` 时 **错误**。 |
| **`range_pair`** | 2 | `(lo, hi)` | 闭区间，长度受 **`range_size`** 约束；`pk_hi[tbl] < 1` 时 **错误**。 |

### 2.2 列值：`col:<name>`（按列绑定，支持多列 INSERT/UPDATE）

| 形式 | arity | 含义 |
|------|-------|------|
| **`col:<column>`** | 1 | 为名为 **`<column>`** 的列生成 **一个** 绑定值，类型与生成规则由 **model 元数据** 决定（见 §3）。 |

- **`<column>`** 必须是当前 model 所描述表上存在的逻辑列：**内置列**（默认 `k`、`c`、`pad`）或 **`prepare.extra_columns[].name`**。
- **与 SQL 列清单对齐**：`INSERT INTO … (a, b, c) VALUES (%s, %s, %s)` 对应  
  `bind: [col:a, col:b, col:c]` —— **占位符顺序与 VALUES 中列顺序一致**，与「谁在 INSERT 列表里先写」一致，而不是与表 DDL 物理顺序绑定。
- **多列 UPDATE**：`SET x = %s, y = %s` 对应 `bind: [col:x, col:y, …]`，再拼 `row_id` 等；**SET 子句里 `%s` 从左到右** 与 `bind` 中列原子顺序一致。

这样 **不再出现 `insert_payload` / `update_payload`** 这类按操作命名的耦合；多列插入/更新只需 **多写几个 `col:…`**。

### 2.3 预留（实现可拒收或单独版本）

| 预留 id | 预期用途 |
|---------|----------|
| **`returning_id`** | 从 `RETURNING`/游标取新 id 写回 `pk_hi` |
| **`literal:<json>`** | 固定常量 |
| **`row_id`** 重复两次 | 两个独立随机 id：若需此语义，用 **`[row_id, row_id]`**（见 §5） |

---

## 3. 列元数据与值生成（`col:<name>`）

实现从 **`BenchModel`** 解析列：

- **内置列** `k`、`c`、`pad`：规则写死在执行器。
- **`prepare.extra_columns`**：用 **`sql_kind`**（`int` / `bigint` / `text` / `varchar`）决定随机整数、随机字符串等。

**按类型的默认生成（建议）**

| sql_kind / 列角色 | 生成策略 |
|-------------------|----------|
| 文本类（含内置 `c`、`pad`） | 生成线程与时间相关的随机字符串。 |
| `int` / `bigint` | 有界随机整数（与现 `UPDATE` extra 列一致）。 |
| `k` | 随机整数（对齐默认 sbtest-like 列）。 |

**`kind` 的作用**：仍用于 **报表分类**（读/写/更新/删），绑定值生成不依赖 `kind`。

---

## 4. 与 `kind` 的关系

- **`kind`**：**吞吐分类**必选。  
- **`col:<name>`**：按列类型生成绑定值；与 `kind` 解耦。

---

## 5. 示例

**点查 / 范围 / 删**（与列无关，同前）

```yaml
bind: [row_id]
bind: [range_pair]
bind: [row_id]
```

**插入多列**

```yaml
sql: "INSERT INTO {table} (k, c, pad) VALUES (%s, %s, %s)"
bind: [col:k, col:c, col:pad]
```

**更新多列 + WHERE**

```yaml
sql: "UPDATE {table} SET k = %s, c = %s WHERE id = %s"
bind: [col:k, col:c, row_id]
```

---

## 6. 校验规则（实现检查清单）

1. `bind` 为列表；无 `%s` 的 SQL 为 `bind: []`。  
2. 各原子 arity 之和 = `%s` 个数。  
3. 每个 **`col:<name>`** 在 model 中有对应列定义（或内置列白名单）。  
4. `range_pair` 与两个 `row_id` 语义不同；后者写 **`[row_id, row_id]`**。

---

## 7. 宏与 `bind` 的次序

1. 替换 `{table}`、`{select_list}` 等宏；  
2. 统计 `%s`；  
3. 按 `bind` 生成参数并 `execute`。

---

*版本：草案 v1.1；**`insert_payload` / `update_payload` 已废弃**，由 **`col:<column>`** 统一表达列值绑定。*

**实现状态**：`bench_model` 在存在 **`sql`** 时解析 **`bind`**；`db_bench.execute_statement_template` 按上表展开参数（与本文一致）。
