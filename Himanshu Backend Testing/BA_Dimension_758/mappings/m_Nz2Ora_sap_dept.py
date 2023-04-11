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
# DBTITLE 1, SAP_DEPT_0


df_0=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        MERCH_DIVISIONAL_ID AS MERCH_DIVISIONAL_ID,
        BUYER_ID AS BUYER_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_DEPT""")

df_0.createOrReplaceTempView("SAP_DEPT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SAP_DEPT_1


df_1=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        MERCH_DIVISIONAL_ID AS MERCH_DIVISIONAL_ID,
        BUYER_ID AS BUYER_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_DEPT_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SAP_DEPT_1")

# COMMAND ----------
# DBTITLE 1, SAP_DEPT


spark.sql("""INSERT INTO SAP_DEPT SELECT SAP_DEPT_ID AS SAP_DEPT_ID,
SAP_DEPT_DESC AS SAP_DEPT_DESC,
SAP_DIVISION_ID AS SAP_DIVISION_ID,
MERCH_DIVISIONAL_ID AS MERCH_DIVISIONAL_ID,
BUYER_ID AS BUYER_ID FROM SQ_Shortcut_to_SAP_DEPT_1""")