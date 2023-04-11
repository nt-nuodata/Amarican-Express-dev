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
# DBTITLE 1, REGION_0


df_0=spark.sql("""
    SELECT
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REGION""")

df_0.createOrReplaceTempView("REGION_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_REGION_1


df_1=spark.sql("""
    SELECT
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        REGION_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_REGION_1")

# COMMAND ----------
# DBTITLE 1, REGION_Ora


spark.sql("""INSERT INTO REGION_Ora SELECT REGION_ID AS REGION_ID,
REGION_DESC AS REGION_DESC FROM SQ_Shortcut_to_REGION_1""")