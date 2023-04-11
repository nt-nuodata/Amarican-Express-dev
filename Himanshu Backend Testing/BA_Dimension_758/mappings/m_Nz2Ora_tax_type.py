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
# DBTITLE 1, TAX_TYPE_0


df_0=spark.sql("""
    SELECT
        TAX_TYPE_ID AS TAX_TYPE_ID,
        TAX_TYPE_DESC AS TAX_TYPE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TAX_TYPE""")

df_0.createOrReplaceTempView("TAX_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TAX_TYPE_1


df_1=spark.sql("""
    SELECT
        TAX_TYPE_ID AS TAX_TYPE_ID,
        TAX_TYPE_DESC AS TAX_TYPE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        TAX_TYPE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_TAX_TYPE_1")

# COMMAND ----------
# DBTITLE 1, TAX_TYPE


spark.sql("""INSERT INTO TAX_TYPE SELECT TAX_TYPE_ID AS TAX_TYPE_ID,
TAX_TYPE_DESC AS TAX_TYPE_DESC FROM SQ_Shortcut_to_TAX_TYPE_1""")