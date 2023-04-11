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
# DBTITLE 1, UPC_0


df_0=spark.sql("""
    SELECT
        UPC_ID AS UPC_ID,
        UPC_CD AS UPC_CD,
        UPC_ADD_DT AS UPC_ADD_DT,
        UPC_DELETE_DT AS UPC_DELETE_DT,
        UPC_REFRESH_DT AS UPC_REFRESH_DT,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        UPC""")

df_0.createOrReplaceTempView("UPC_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UPC_1


df_1=spark.sql("""
    SELECT
        UPC_ID AS UPC_ID,
        UPC_ADD_DT AS UPC_ADD_DT,
        UPC_DELETE_DT AS UPC_DELETE_DT,
        UPC_REFRESH_DT AS UPC_REFRESH_DT,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UPC_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_UPC_1")

# COMMAND ----------
# DBTITLE 1, UPC


spark.sql("""INSERT INTO UPC SELECT UPC_ID AS UPC_ID,
UPC_ADD_DT AS UPC_ADD_DT,
UPC_DELETE_DT AS UPC_DELETE_DT,
UPC_REFRESH_DT AS UPC_REFRESH_DT,
PRODUCT_ID AS PRODUCT_ID,
SKU_NBR AS SKU_NBR FROM SQ_Shortcut_to_UPC_1""")