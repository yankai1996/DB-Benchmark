#!/usr/bin/env sysbench

-- Standalone MySQL/MariaDB workload script.
-- Default mode: batch_update_by_pk with unique random PKs per event.

local DEFAULT_TABLE_NAME = "sbtest_custom"
local DEFAULT_TABLES = 1
local DEFAULT_TABLE_SIZE = 10000
local DEFAULT_COLUMN_COUNT = 4
local DEFAULT_SELECT_COL_COUNT = DEFAULT_COLUMN_COUNT
local DEFAULT_UPDATE_COL_COUNT = DEFAULT_COLUMN_COUNT
local DEFAULT_INSERT_COL_COUNT = DEFAULT_COLUMN_COUNT
local DEFAULT_WORKLOAD_MODE = "batch_update_by_pk"
local DEFAULT_ROWS_PER_UPDATE = 10
local DEFAULT_WRITE_WEIGHTS = "0,1,0" -- insert,update,delete
local DEFAULT_INSERT_BATCH_SIZE = 1000
local RAND_INT_MAX = 2147483647
local STR_LEN = 20
local STR_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

sysbench.cmdline.options = {
  table_name = {"Table name (or stem when tables > 1).", DEFAULT_TABLE_NAME},
  tables = {"Number of tables to use.", DEFAULT_TABLES},
  table_size = {"Rows per table inserted during prepare.", DEFAULT_TABLE_SIZE},
  column_count = {"Number of non-PK columns as col1..colN.", DEFAULT_COLUMN_COUNT},
  select_col_count = {"First N columns used by SELECT.", DEFAULT_SELECT_COL_COUNT},
  update_col_count = {"First N columns used by UPDATE.", DEFAULT_UPDATE_COL_COUNT},
  insert_col_count = {"First N columns used by INSERT.", DEFAULT_INSERT_COL_COUNT},
  workload_mode = {"Workload mode: batch_update_by_pk|mixed_write.", DEFAULT_WORKLOAD_MODE},
  rows_per_update = {"UPDATE statements per event (unique PKs).", DEFAULT_ROWS_PER_UPDATE},
  write_weights = {"insert,update,delete integer weights.", DEFAULT_WRITE_WEIGHTS},
  insert_batch_size = {"Rows per INSERT VALUES batch in prepare.", DEFAULT_INSERT_BATCH_SIZE}
}

local cfg = nil
local drv = nil
local con = nil

local function fail(msg)
  error("sysbench_write.lua: " .. msg)
end

local function parse_int(name, val, min_value)
  local n = tonumber(val)
  if n == nil then
    fail(name .. " must be a number, got: " .. tostring(val))
  end
  n = math.floor(n)
  if min_value ~= nil and n < min_value then
    fail(name .. " must be >= " .. tostring(min_value) .. ", got: " .. tostring(n))
  end
  return n
end

local function trim(s)
  return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function validate_identifier(name, what)
  if not string.match(name, "^[A-Za-z_][A-Za-z0-9_]*$") then
    fail(what .. " has invalid identifier syntax: " .. tostring(name))
  end
end

local function split_csv(s)
  local out = {}
  for part in string.gmatch(s, "([^,]+)") do
    out[#out + 1] = trim(part)
  end
  return out
end

local function parse_weights(raw)
  local parts = split_csv(raw)
  if #parts ~= 3 then
    fail("write_weights must be exactly 3 integers: insert,update,delete")
  end
  local ins = parse_int("write_weights.insert", parts[1], 0)
  local upd = parse_int("write_weights.update", parts[2], 0)
  local dele = parse_int("write_weights.delete", parts[3], 0)
  local total = ins + upd + dele
  if total <= 0 then
    fail("write_weights total must be > 0")
  end
  return {insert = ins, update = upd, delete = dele, total = total}
end

local function column_name(i)
  return "col" .. tostring(i)
end

local function column_kind(i)
  if (i % 2) == 1 then
    return "int"
  end
  return "char20"
end

local function random_char20()
  local out = {}
  local m = string.len(STR_CHARS)
  for _ = 1, STR_LEN do
    local idx = sysbench.rand.uniform(1, m)
    out[#out + 1] = string.sub(STR_CHARS, idx, idx)
  end
  return table.concat(out, "")
end

local function sql_quote(v)
  local s = tostring(v)
  s = string.gsub(s, "'", "''")
  return "'" .. s .. "'"
end

local function column_random_sql_value(i)
  if column_kind(i) == "int" then
    return tostring(sysbench.rand.uniform(1, RAND_INT_MAX))
  end
  return sql_quote(random_char20())
end

local function clamp_count(v, max_v)
  if v < 0 then
    return 0
  end
  if v > max_v then
    return max_v
  end
  return v
end

local function build_first_n_columns(n)
  local cols = {}
  for i = 1, n do
    cols[#cols + 1] = column_name(i)
  end
  return cols
end

local function build_config()
  if cfg ~= nil then
    return cfg
  end

  local table_name = tostring(sysbench.opt.table_name)
  validate_identifier(table_name, "table_name")

  local tables = parse_int("tables", sysbench.opt.tables, 1)
  local table_size = parse_int("table_size", sysbench.opt.table_size, 1)
  local column_count = parse_int("column_count", sysbench.opt.column_count, 1)
  local rows_per_update = parse_int("rows_per_update", sysbench.opt.rows_per_update, 1)
  local insert_batch_size = parse_int("insert_batch_size", sysbench.opt.insert_batch_size, 1)
  local select_col_count = parse_int("select_col_count", sysbench.opt.select_col_count, 0)
  local update_col_count = parse_int("update_col_count", sysbench.opt.update_col_count, 1)
  local insert_col_count = parse_int("insert_col_count", sysbench.opt.insert_col_count, 1)
  local workload_mode = tostring(sysbench.opt.workload_mode or DEFAULT_WORKLOAD_MODE)

  if workload_mode ~= "batch_update_by_pk" and workload_mode ~= "mixed_write" then
    fail("workload_mode must be one of: batch_update_by_pk, mixed_write")
  end
  if rows_per_update > table_size then
    fail("rows_per_update cannot exceed table_size when PKs must be unique per event")
  end

  select_col_count = clamp_count(select_col_count, column_count)
  update_col_count = clamp_count(update_col_count, column_count)
  insert_col_count = clamp_count(insert_col_count, column_count)

  local weights = parse_weights(tostring(sysbench.opt.write_weights))
  cfg = {
    table_name = table_name,
    tables = tables,
    table_size = table_size,
    column_count = column_count,
    workload_mode = workload_mode,
    rows_per_update = rows_per_update,
    select_col_count = select_col_count,
    update_col_count = update_col_count,
    insert_col_count = insert_col_count,
    select_columns = build_first_n_columns(select_col_count),
    update_columns = build_first_n_columns(update_col_count),
    insert_columns = build_first_n_columns(insert_col_count),
    write_weights = weights,
    insert_batch_size = insert_batch_size
  }
  return cfg
end

local function ensure_connection()
  if drv == nil then
    drv = sysbench.sql.driver()
  end
  if con == nil then
    con = drv:connect()
  end
end

local function close_connection()
  if con ~= nil then
    con:disconnect()
    con = nil
  end
end

local function table_name_by_index(stem, idx, total)
  if total == 1 then
    return stem
  end
  return stem .. tostring(idx)
end

local function random_table_name(local_cfg)
  local idx = 1
  if local_cfg.tables > 1 then
    idx = sysbench.rand.uniform(1, local_cfg.tables)
  end
  return table_name_by_index(local_cfg.table_name, idx, local_cfg.tables)
end

local function build_create_table_sql(tbl, local_cfg)
  local defs = {"id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY"}
  for i = 1, local_cfg.column_count do
    local c = column_name(i)
    if column_kind(i) == "int" then
      defs[#defs + 1] = c .. " INT NOT NULL DEFAULT 0"
    else
      defs[#defs + 1] = c .. " CHAR(20) NOT NULL DEFAULT ''"
    end
  end
  return string.format("CREATE TABLE IF NOT EXISTS %s (%s)", tbl, table.concat(defs, ", "))
end

local function build_prepare_values_row(local_cfg)
  local vals = {}
  for i = 1, local_cfg.column_count do
    vals[#vals + 1] = column_random_sql_value(i)
  end
  return "(" .. table.concat(vals, ", ") .. ")"
end

local function prepare_single_table(tbl, local_cfg)
  con:query(build_create_table_sql(tbl, local_cfg))

  local inserted = 0
  local all_cols = build_first_n_columns(local_cfg.column_count)
  local cols_sql = table.concat(all_cols, ", ")
  while inserted < local_cfg.table_size do
    local n = math.min(local_cfg.insert_batch_size, local_cfg.table_size - inserted)
    local values = {}
    for _ = 1, n do
      values[#values + 1] = build_prepare_values_row(local_cfg)
    end
    local sql = string.format("INSERT INTO %s (%s) VALUES %s", tbl, cols_sql, table.concat(values, ", "))
    con:query(sql)
    inserted = inserted + n
  end
end

local function unique_random_ids(count, max_id)
  local ids = {}
  local seen = {}
  while #ids < count do
    local id = sysbench.rand.uniform(1, max_id)
    if not seen[id] then
      seen[id] = true
      ids[#ids + 1] = id
    end
  end
  return ids
end

local function build_set_clause(columns)
  local parts = {}
  for _, c in ipairs(columns) do
    local idx = tonumber(string.match(c, "^col(%d+)$"))
    if idx == nil then
      fail("invalid generated column name in set clause: " .. tostring(c))
    end
    parts[#parts + 1] = string.format("%s = %s", c, column_random_sql_value(idx))
  end
  return table.concat(parts, ", ")
end

local function op_select_by_pk(tbl, id, local_cfg)
  local cols
  if #local_cfg.select_columns == 0 then
    cols = "id"
  else
    cols = "id, " .. table.concat(local_cfg.select_columns, ", ")
  end
  local sql = string.format("SELECT %s FROM %s WHERE id = %d", cols, tbl, id)
  con:query(sql)
end

local function op_insert_one(tbl, local_cfg)
  if #local_cfg.insert_columns == 0 then
    con:query("INSERT INTO " .. tbl .. " () VALUES ()")
    return
  end
  local vals = {}
  for _, c in ipairs(local_cfg.insert_columns) do
    local idx = tonumber(string.match(c, "^col(%d+)$"))
    vals[#vals + 1] = column_random_sql_value(idx)
  end
  local sql = string.format(
    "INSERT INTO %s (%s) VALUES (%s)",
    tbl,
    table.concat(local_cfg.insert_columns, ", "),
    table.concat(vals, ", ")
  )
  con:query(sql)
end

local function op_update_by_pk(tbl, id, local_cfg)
  local set_clause = build_set_clause(local_cfg.update_columns)
  local sql = string.format("UPDATE %s SET %s WHERE id = %d", tbl, set_clause, id)
  con:query(sql)
end

local function op_delete_by_pk(tbl, id)
  con:query(string.format("DELETE FROM %s WHERE id = %d", tbl, id))
end

local function pick_write_op(local_cfg)
  local w = local_cfg.write_weights
  local r = sysbench.rand.uniform(1, w.total)
  if r <= w.insert then
    return "insert"
  end
  if r <= (w.insert + w.update) then
    return "update"
  end
  return "delete"
end

local function run_batch_update_by_pk(tbl, local_cfg)
  local ids = unique_random_ids(local_cfg.rows_per_update, local_cfg.table_size)
  for _, id in ipairs(ids) do
    op_update_by_pk(tbl, id, local_cfg)
  end
end

local function dispatch_workload(tbl, local_cfg)
  if local_cfg.workload_mode == "batch_update_by_pk" then
    run_batch_update_by_pk(tbl, local_cfg)
    return
  end

  -- Extensible skeleton for mixed write routing.
  local op = pick_write_op(local_cfg)
  if op == "insert" then
    for _ = 1, local_cfg.rows_per_update do
      op_insert_one(tbl, local_cfg)
    end
  elseif op == "update" then
    run_batch_update_by_pk(tbl, local_cfg)
  else
    local ids = unique_random_ids(local_cfg.rows_per_update, local_cfg.table_size)
    for _, id in ipairs(ids) do
      op_delete_by_pk(tbl, id)
    end
  end
end

function thread_init()
  build_config()
  ensure_connection()
end

function thread_done()
  close_connection()
end

function prepare()
  local local_cfg = build_config()
  ensure_connection()
  for i = 1, local_cfg.tables do
    local tbl = table_name_by_index(local_cfg.table_name, i, local_cfg.tables)
    sysbench.log_info("prepare table: " .. tbl)
    prepare_single_table(tbl, local_cfg)
  end
end

function cleanup()
  local local_cfg = build_config()
  ensure_connection()
  for i = 1, local_cfg.tables do
    local tbl = table_name_by_index(local_cfg.table_name, i, local_cfg.tables)
    con:query("DROP TABLE IF EXISTS " .. tbl)
  end
end

function event()
  local local_cfg = build_config()
  ensure_connection()
  local tbl = random_table_name(local_cfg)
  dispatch_workload(tbl, local_cfg)
end
