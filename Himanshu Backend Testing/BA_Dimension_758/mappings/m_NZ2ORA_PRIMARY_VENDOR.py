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
# DBTITLE 1, PRIMARY_VENDOR_0


df_0=spark.sql("""
    SELECT
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PRIMARY_VENDOR""")

df_0.createOrReplaceTempView("PRIMARY_VENDOR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_PRIMARY_VENDOR_1


df_1=spark.sql("""
    SELECT
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PRIMARY_VENDOR_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_PRIMARY_VENDOR_1")

# COMMAND ----------
# DBTITLE 1, PRIMARY_VENDOR


spark.sql("""INSERT INTO PRIMARY_VENDOR SELECT PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME FROM SQ_Shortcut_To_PRIMARY_VENDOR_1""")