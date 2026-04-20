#!/usr/bin/env sysbench

-- Execute one transaction per event with a fixed SQL list.
-- Edit FIXED_TXN_SQL below to match your business transaction.

local DEFAULT_TABLE_NAME = "sbtest"

-- Transaction SQL sequence (executed in order inside BEGIN/COMMIT).
-- `{table}` placeholder is replaced by --table_name.
local FIXED_TXN_SQL = {
  "UPDATE {table} SET col1 = col1 + 1 WHERE id = 1",
  "UPDATE {table} SET col1 = col1 + 1 WHERE id = 2",
  "SELECT id, col1 FROM {table} WHERE id = 1",
}

sysbench.cmdline.options = {
  table_name = {"Table name used by {table} placeholder.", DEFAULT_TABLE_NAME},
}

local drv = nil
local con = nil
local prepared_sql = nil

local function fail(msg)
  error("sysbench_txn_fixed.lua: " .. msg)
end

local function validate_identifier(name, what)
  if not string.match(name, "^[A-Za-z_][A-Za-z0-9_]*$") then
    fail(what .. " has invalid identifier syntax: " .. tostring(name))
  end
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

local function build_prepared_sql()
  if prepared_sql ~= nil then
    return prepared_sql
  end
  local table_name = tostring(sysbench.opt.table_name or DEFAULT_TABLE_NAME)
  validate_identifier(table_name, "table_name")
  local out = {}
  for i, sql in ipairs(FIXED_TXN_SQL) do
    local s = tostring(sql or "")
    if s == "" then
      fail("FIXED_TXN_SQL[" .. tostring(i) .. "] is empty")
    end
    out[#out + 1] = s:gsub("{table}", table_name)
  end
  prepared_sql = out
  return prepared_sql
end

function thread_init()
  ensure_connection()
  build_prepared_sql()
end

function thread_done()
  close_connection()
end

function event()
  local sqls = build_prepared_sql()
  con:query("BEGIN")
  local ok, err = pcall(function()
    for _, s in ipairs(sqls) do
      con:query(s)
    end
  end)
  if ok then
    con:query("COMMIT")
    return
  end

  pcall(function()
    con:query("ROLLBACK")
  end)
  error(err)
end
