-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Inspect data

-- COMMAND ----------

USE CATALOG manufacturing_dev;
USE SCHEMA work_agent_barney;

-- COMMAND ----------

SELECT
  *
FROM
  master_sensory_ingredient_statement_bronze;

-- COMMAND ----------

SELECT
  *
FROM
  master_sensory_panel_benchmark_bronze;

-- COMMAND ----------

SELECT
  *
FROM
  master_sensory_panel_dept_info_bronze
WHERE
  attributes_factors_to_be_tested IS NOT NULL;

-- COMMAND ----------

SELECT
  *
FROM
  master_sensory_panel_info_1_bronze
WHERE
  row_name IS NOT NULL;

-- COMMAND ----------

SELECT
  *
FROM
  master_sensory_panel_info_2_bronze
WHERE
  row_name IS NOT NULL;

-- COMMAND ----------

SELECT
  *
FROM
  master_sensory_panel_summary_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Join tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW test_item_pivot AS
SELECT
  FIRST(spec_number) AS item_spec_number,
  FIRST(type) AS item_type,
  FIRST(product_identification) AS item_product_id,
  FIRST(brand) AS item_brand,
  FIRST(plant_supplier) AS item_plant_supplier,
  FIRST(formula_number_code) AS item_formula_number_code,
  FIRST(lpad(CAST(CAST(upcnumber AS INT) AS STRING), 13, '0')) AS item_upc_number
FROM
  master_sensory_test_item_bronze
GROUP BY
  spec_number;

SELECT * FROM test_item_pivot;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW summary_pivot AS
SELECT
  spec_number AS summary_spec_number,
  FIRST(value) FILTER (WHERE row_name = 'Summary_Results') AS summary_summary_results,
  FIRST(value) FILTER (WHERE row_name = 'Other_Findings') AS summary_other_findings,
  FIRST(value) FILTER (WHERE row_name = 'Special_Notes') AS summary_special_notes,
  FIRST(value) FILTER (WHERE row_name = 'Met_Expectation') AS summary_met_expectation,
  FIRST(value) FILTER (WHERE row_name = 'Panel_Pass_Fail') AS summary_panel_pass_fail,
  FIRST(value) FILTER (WHERE row_name = 'Cost_of_Panel') AS summary_cost_of_panel,
  FIRST(value) FILTER (WHERE row_name = 'Amount_of_Prod_Used') AS summary_amount_of_prod_used
FROM
  master_sensory_panel_summary_bronze
GROUP BY
  spec_number;

SELECT * FROM summary_pivot;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dept_info_pivot AS
SELECT
  spec_number AS dept_info_spec_number,
  FIRST(test_methodology_check_all_that_apply) FILTER(WHERE column_name = 'Value') AS dept_info_test_methodology,
  FIRST(test_methodology_check_all_that_apply) FILTER(WHERE column_name = 'Comments') AS dept_info_test_methodology_comments,
  FIRST(attributes_factors_to_be_tested) FILTER(WHERE column_name = 'Value') AS dept_info_attributes_factors_to_be_tested,
  FIRST(attributes_factors_to_be_tested) FILTER(WHERE column_name = 'Comments') AS dept_info_attributes_factors_to_be_tested_comments,
  FIRST(TO_DATE(date_of_panel, 'yyyy/MM/dd HH:mm:ss')) FILTER(WHERE column_name = 'Value') AS dept_info_date_of_panel
FROM
  master_sensory_panel_dept_info_bronze
WHERE
  attributes_factors_to_be_tested IS NOT NULL
GROUP BY
  spec_number;

SELECT * FROM dept_info_pivot;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW benchmark_pivot AS
SELECT
  spec_number AS benchmark_spec_number,
  FIRST(product_identification) FILTER(WHERE type = 'Benchmark') AS benchmark_product_id,
  FIRST(brand) FILTER(WHERE type = 'Benchmark') AS benchmark_brand,
  FIRST(company) FILTER(WHERE type = 'Benchmark') AS benchmark_company,
  FIRST(formula_code) FILTER(WHERE type = 'Benchmark') AS benchmark_formula_code,
  FIRST(lpad(CAST(CAST(upcnumber AS INT) AS STRING), 13, '0')) FILTER(WHERE type = 'Benchmark') AS benchmark_upc_number
FROM
  master_sensory_panel_benchmark_bronze
WHERE
  type = 'Benchmark'
GROUP BY
  spec_number;

SELECT * FROM benchmark_pivot;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW requested_info_2_pivot AS
SELECT
  spec_number AS requested_info_2_spec_number,
  MAX(TO_DATE(value, 'yyyy/MM/dd HH:mm:ss')) FILTER(WHERE row_name = 'Date_of_Formal_Cutting') AS data_of_formal_cutting
FROM
  master_sensory_requested_info_2_bronze
GROUP BY
  spec_number;

SELECT * FROM requested_info_2_pivot;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW ingredient_statement AS
SELECT
  spec_number AS ingredient_statement_spec_number,
  COLLECT_LIST(STRUCT(*)) AS ingredient_statement
FROM
  master_sensory_ingredient_statement_bronze
GROUP BY
  spec_number;

SELECT * FROM ingredient_statement;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sensory AS
SELECT
  -- test_item
  item_spec_number,
  item_type,
  item_product_id,
  item_brand,
  item_plant_supplier,
  item_formula_number_code,
  item_upc_number,

  -- summary
  summary_summary_results,
  summary_other_findings,
  summary_special_notes,
  summary_met_expectation,
  summary_panel_pass_fail,

  -- dept_info
  dept_info_test_methodology,
  dept_info_test_methodology_comments,
  dept_info_attributes_factors_to_be_tested,
  dept_info_attributes_factors_to_be_tested_comments,
  dept_info_date_of_panel,

  -- benchmark
  benchmark_product_id,
  benchmark_brand,
  benchmark_company,
  benchmark_formula_code,
  benchmark_upc_number,

  -- requested_info_2
  data_of_formal_cutting,

  -- ingredient statement
  ingredient_statement
FROM
  test_item_pivot
LEFT JOIN
  summary_pivot
ON
  summary_spec_number = item_spec_number
LEFT JOIN
  dept_info_pivot
ON
  dept_info_spec_number = item_spec_number
LEFT JOIN
  benchmark_pivot
ON
  benchmark_spec_number = item_spec_number
LEFT JOIN
  requested_info_2_pivot
ON
  requested_info_2_spec_number = item_spec_number
LEFT JOIN
  ingredient_statement
ON
  ingredient_statement_spec_number = item_spec_number;

SELECT * FROM sensory;

-- COMMAND ----------

SELECT COUNT(*) FROM sensory;

-- COMMAND ----------

CREATE OR REPLACE TABLE master_sensory_joined_wip AS
SELECT * FROM sensory;