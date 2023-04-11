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
# DBTITLE 1, SAP_CLASS_0


df_0=spark.sql("""
    SELECT
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_CLASS_DESC AS SAP_CLASS_DESC,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_CLASS""")

df_0.createOrReplaceTempView("SAP_CLASS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SAP_CLASS_1


df_1=spark.sql("""
    SELECT
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_CLASS_DESC AS SAP_CLASS_DESC,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_CLASS_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SAP_CLASS_1")

# COMMAND ----------
# DBTITLE 1, SAP_CLASS_Ora


spark.sql("""INSERT INTO SAP_CLASS_Ora SELECT SAP_CLASS_ID AS SAP_CLASS_ID,
SAP_CLASS_DESC AS SAP_CLASS_DESC,
SAP_DEPT_ID AS SAP_DEPT_ID FROM SQ_Shortcut_to_SAP_CLASS_1""")