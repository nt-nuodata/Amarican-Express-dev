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
# DBTITLE 1, SKU_STATUS_0


df_0=spark.sql("""
    SELECT
        STATUS_ID AS STATUS_ID,
        STATUS_NAME AS STATUS_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_STATUS""")

df_0.createOrReplaceTempView("SKU_STATUS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_STATUS_1


df_1=spark.sql("""
    SELECT
        STATUS_ID AS STATUS_ID,
        STATUS_NAME AS STATUS_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_STATUS_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_SKU_STATUS_1")

# COMMAND ----------
# DBTITLE 1, SKU_STATUS


spark.sql("""INSERT INTO SKU_STATUS SELECT STATUS_ID AS STATUS_ID,
STATUS_NAME AS STATUS_NAME FROM SQ_Shortcut_To_SKU_STATUS_1""")