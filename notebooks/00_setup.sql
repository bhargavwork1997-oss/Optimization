-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup
-- MAGIC Creates the `dpo` schema and the DBU price reference table.
-- MAGIC
-- MAGIC **Free Edition:** the default catalog is `workspace`. On a client workspace, change `workspace` to the client's catalog in every notebook (search/replace `workspace.dpo`).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.dpo
COMMENT 'Databricks Processing Optimization (DPO) accelerator';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DBU price reference
-- MAGIC Illustrative list prices (USD per DBU). **Replace with the client's contracted rates** before any assessment.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.dpo.ref_sku_price (
  sku          STRING  COMMENT 'Normalized SKU family',
  usd_per_dbu  DOUBLE  COMMENT 'Price per DBU in USD'
) COMMENT 'DBU price per SKU family';

INSERT INTO workspace.dpo.ref_sku_price VALUES
  ('JOBS_COMPUTE',        0.15),
  ('ALL_PURPOSE_COMPUTE', 0.40),
  ('SQL_PRO',             0.55),
  ('DLT_ADVANCED',        0.36),
  ('SERVERLESS_JOBS',     0.35);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optional: check system tables
-- MAGIC On a paid workspace with Unity Catalog system tables enabled, this returns rows and you can use `01b_system_tables_source`. On Free Edition it may be empty or denied — that's expected; use the synthetic generator (`01_generate_logs`).

-- COMMAND ----------

-- SELECT * FROM system.billing.usage LIMIT 10;
SELECT 'setup complete' AS status;
