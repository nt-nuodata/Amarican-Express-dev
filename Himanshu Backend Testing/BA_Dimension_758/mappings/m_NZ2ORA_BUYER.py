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
# DBTITLE 1, BUYER_0


df_0=spark.sql("""
    SELECT
        BUYER_ID AS BUYER_ID,
        BUYER_NAME AS BUYER_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BUYER""")

df_0.createOrReplaceTempView("BUYER_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_BUYER_1


df_1=spark.sql("""
    SELECT
        BUYER_ID AS BUYER_ID,
        BUYER_NAME AS BUYER_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        BUYER_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_BUYER_1")

# COMMAND ----------
# DBTITLE 1, BUYER


spark.sql("""INSERT INTO BUYER SELECT BUYER_ID AS BUYER_ID,
BUYER_NAME AS BUYER_NAME FROM SQ_Shortcut_To_BUYER_1""")