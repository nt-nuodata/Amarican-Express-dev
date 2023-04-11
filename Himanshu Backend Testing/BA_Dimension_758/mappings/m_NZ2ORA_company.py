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
# DBTITLE 1, COMPANY_0


df_0=spark.sql("""
    SELECT
        COMPANY_ID AS COMPANY_ID,
        COMPANY_DESC AS COMPANY_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        COMPANY""")

df_0.createOrReplaceTempView("COMPANY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_COMPANY_1


df_1=spark.sql("""
    SELECT
        COMPANY_ID AS COMPANY_ID,
        COMPANY_DESC AS COMPANY_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        COMPANY_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_COMPANY_1")

# COMMAND ----------
# DBTITLE 1, COMPANY


spark.sql("""INSERT INTO COMPANY SELECT COMPANY_ID AS COMPANY_ID,
COMPANY_DESC AS COMPANY_DESC FROM SQ_Shortcut_to_COMPANY_1""")