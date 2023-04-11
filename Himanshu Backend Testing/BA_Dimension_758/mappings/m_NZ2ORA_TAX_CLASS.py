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
# DBTITLE 1, TAX_CLASS_0


df_0=spark.sql("""
    SELECT
        TAX_CLASS_ID AS TAX_CLASS_ID,
        TAX_CLASS_DESC AS TAX_CLASS_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TAX_CLASS""")

df_0.createOrReplaceTempView("TAX_CLASS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TAX_CLASS_1


df_1=spark.sql("""
    SELECT
        TAX_CLASS_ID,
        TAX_CLASS_DESC 
    FROM
        TAX_CLASS""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_TAX_CLASS_1")

# COMMAND ----------
# DBTITLE 1, TAX_CLASS


spark.sql("""INSERT INTO TAX_CLASS SELECT TAX_CLASS_ID AS TAX_CLASS_ID,
TAX_CLASS_DESC AS TAX_CLASS_DESC FROM SQ_Shortcut_to_TAX_CLASS_1""")