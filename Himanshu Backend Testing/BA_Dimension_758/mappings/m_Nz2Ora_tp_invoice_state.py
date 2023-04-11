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
# DBTITLE 1, TP_INVOICE_STATE_0


df_0=spark.sql("""
    SELECT
        INVOICE_STATE_ID AS INVOICE_STATE_ID,
        INVOICE_STATE_DESC AS INVOICE_STATE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TP_INVOICE_STATE""")

df_0.createOrReplaceTempView("TP_INVOICE_STATE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TP_INVOICE_STATE_1


df_1=spark.sql("""
    SELECT
        INVOICE_STATE_ID AS INVOICE_STATE_ID,
        INVOICE_STATE_DESC AS INVOICE_STATE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        TP_INVOICE_STATE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_TP_INVOICE_STATE_1")

# COMMAND ----------
# DBTITLE 1, TP_INVOICE_STATE


spark.sql("""INSERT INTO TP_INVOICE_STATE SELECT INVOICE_STATE_ID AS INVOICE_STATE_ID,
INVOICE_STATE_DESC AS INVOICE_STATE_DESC FROM SQ_Shortcut_to_TP_INVOICE_STATE_1""")