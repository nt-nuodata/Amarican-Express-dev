# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Transformation not handled

# COMMAND ----------
# DBTITLE 1, SKU_CASE_DIM_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        CASE_UNIT_CNT AS CASE_UNIT_CNT,
        CASE_VOLUME_AMT AS CASE_VOLUME_AMT,
        CASE_WEIGHT_GROSS_AMT AS CASE_WEIGHT_GROSS_AMT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_CASE_DIM""")

df_0.createOrReplaceTempView("SKU_CASE_DIM_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_CASE_DIM_1


df_1=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        CASE_UNIT_CNT AS CASE_UNIT_CNT,
        CASE_VOLUME_AMT AS CASE_VOLUME_AMT,
        CASE_WEIGHT_GROSS_AMT AS CASE_WEIGHT_GROSS_AMT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_CASE_DIM_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_CASE_DIM_1")

# COMMAND ----------
# DBTITLE 1, SKU_CASE_DIM


spark.sql("""INSERT INTO SKU_CASE_DIM SELECT PRODUCT_ID AS PRODUCT_ID,
CASE_UNIT_CNT AS CASE_UNIT_CNT,
CASE_VOLUME_AMT AS CASE_VOLUME_AMT,
CASE_WEIGHT_GROSS_AMT AS CASE_WEIGHT_GROSS_AMT FROM SQ_Shortcut_to_SKU_CASE_DIM_1""")