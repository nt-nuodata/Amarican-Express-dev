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
# DBTITLE 1, SALES_TYPE_0


df_0=spark.sql("""
    SELECT
        SALES_TYPE_ID AS SALES_TYPE_ID,
        SALES_TYPE_CODE AS SALES_TYPE_CODE,
        SALES_TYPE_DESC AS SALES_TYPE_DESC,
        POS_TXN_TYPE_ID AS POS_TXN_TYPE_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TYPE""")

df_0.createOrReplaceTempView("SALES_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SALES_TYPE_1


df_1=spark.sql("""
    SELECT
        SALES_TYPE_ID AS SALES_TYPE_ID,
        SALES_TYPE_CODE AS SALES_TYPE_CODE,
        SALES_TYPE_DESC AS SALES_TYPE_DESC,
        POS_TXN_TYPE_ID AS POS_TXN_TYPE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SALES_TYPE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SALES_TYPE_1")

# COMMAND ----------
# DBTITLE 1, SALES_TYPE


spark.sql("""INSERT INTO SALES_TYPE SELECT SALES_TYPE_ID AS SALES_TYPE_ID,
SALES_TYPE_CODE AS SALES_TYPE_CODE,
SALES_TYPE_DESC AS SALES_TYPE_DESC,
POS_TXN_TYPE_ID AS POS_TXN_TYPE_ID FROM SQ_Shortcut_to_SALES_TYPE_1""")