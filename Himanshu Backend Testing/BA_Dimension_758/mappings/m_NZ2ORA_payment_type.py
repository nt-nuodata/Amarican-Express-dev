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
# DBTITLE 1, PAYMENT_TYPE_0


df_0=spark.sql("""
    SELECT
        PAYMENT_TYPE_ID AS PAYMENT_TYPE_ID,
        PAYMENT_TYPE_DESC AS PAYMENT_TYPE_DESC,
        TENDER_TYPE_ID AS TENDER_TYPE_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PAYMENT_TYPE""")

df_0.createOrReplaceTempView("PAYMENT_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PAYMENT_TYPE_1


df_1=spark.sql("""
    SELECT
        PAYMENT_TYPE_ID AS PAYMENT_TYPE_ID,
        PAYMENT_TYPE_DESC AS PAYMENT_TYPE_DESC,
        TENDER_TYPE_ID AS TENDER_TYPE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PAYMENT_TYPE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PAYMENT_TYPE_1")

# COMMAND ----------
# DBTITLE 1, PAYMENT_TYPE


spark.sql("""INSERT INTO PAYMENT_TYPE SELECT PAYMENT_TYPE_ID AS PAYMENT_TYPE_ID,
PAYMENT_TYPE_DESC AS PAYMENT_TYPE_DESC,
TENDER_TYPE_ID AS TENDER_TYPE_ID FROM SQ_Shortcut_to_PAYMENT_TYPE_1""")