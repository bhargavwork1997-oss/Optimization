-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver: cost attribution
-- MAGIC Cleans raw usage and attaches USD cost per job / cluster / day.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.dpo.usage_cost
COMMENT 'Daily usage with USD cost per job and cluster'
AS
SELECT
  r.*,
  p.usd_per_dbu,
  ROUND(r.dbus * p.usd_per_dbu, 2)                AS cost_usd,
  ROUND(r.dbus / NULLIF(r.runtime_hours, 0), 2)   AS dbu_per_hour
FROM workspace.dpo.raw_usage_logs r
LEFT JOIN workspace.dpo.ref_sku_price p USING (sku)
WHERE r.dbus > 0;

-- COMMAND ----------

-- Data quality: any SKU without a price shows up here and must be added to ref_sku_price
SELECT sku, COUNT(*) AS unpriced_rows
FROM workspace.dpo.usage_cost
WHERE usd_per_dbu IS NULL
GROUP BY sku;

-- COMMAND ----------

SELECT usage_date, ROUND(SUM(cost_usd), 2) AS daily_cost_usd
FROM workspace.dpo.usage_cost
GROUP BY usage_date ORDER BY usage_date;
