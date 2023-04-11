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
# DBTITLE 1, REASON_0


df_0=spark.sql("""
    SELECT
        REASON_ID AS REASON_ID,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        REASON_DESC AS REASON_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REASON""")

df_0.createOrReplaceTempView("REASON_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_REASON_1


df_1=spark.sql("""
    SELECT
        REASON_ID AS REASON_ID,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        REASON_DESC AS REASON_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        REASON_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_REASON_1")

# COMMAND ----------
# DBTITLE 1, REASON


spark.sql("""INSERT INTO REASON SELECT REASON_ID AS REASON_ID,
SALES_TYPE_ID AS SALES_TYPE_ID,
REASON_DESC AS REASON_DESC FROM SQ_Shortcut_to_REASON_1""")