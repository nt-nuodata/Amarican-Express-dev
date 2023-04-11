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
# DBTITLE 1, SAP_DIVISION_0


df_0=spark.sql("""
    SELECT
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        MERCH_SVP_ID AS MERCH_SVP_ID,
        MERCH_VP_ID AS MERCH_VP_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_DIVISION""")

df_0.createOrReplaceTempView("SAP_DIVISION_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SAP_DIVISION_1


df_1=spark.sql("""
    SELECT
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        MERCH_SVP_ID AS MERCH_SVP_ID,
        MERCH_VP_ID AS MERCH_VP_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_DIVISION_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SAP_DIVISION_1")

# COMMAND ----------
# DBTITLE 1, SAP_DIVISION_Ora


spark.sql("""INSERT INTO SAP_DIVISION_Ora SELECT SAP_DIVISION_ID AS SAP_DIVISION_ID,
SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
MERCH_SVP_ID AS MERCH_SVP_ID,
MERCH_VP_ID AS MERCH_VP_ID FROM SQ_Shortcut_to_SAP_DIVISION_1""")