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
# DBTITLE 1, STATE_0


df_0=spark.sql("""
    SELECT
        STATE_CD AS STATE_CD,
        STATE_NAME AS STATE_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STATE""")

df_0.createOrReplaceTempView("STATE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_STATE_1


df_1=spark.sql("""
    SELECT
        STATE_CD AS STATE_CD,
        STATE_NAME AS STATE_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        STATE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_STATE_1")

# COMMAND ----------
# DBTITLE 1, STATE


spark.sql("""INSERT INTO STATE SELECT STATE_CD AS STATE_CD,
STATE_NAME AS STATE_NAME FROM SQ_Shortcut_to_STATE_1""")