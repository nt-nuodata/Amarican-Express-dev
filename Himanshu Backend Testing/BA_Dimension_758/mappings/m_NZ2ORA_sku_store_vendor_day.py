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
# DBTITLE 1, SKU_STORE_VENDOR_DAY_0


df_0=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        DELETE_IND AS DELETE_IND,
        SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
        SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCURE_STORE_NBR AS PROCURE_STORE_NBR,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_STORE_VENDOR_DAY""")

df_0.createOrReplaceTempView("SKU_STORE_VENDOR_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_STORE_VENDOR_DAY_1


df_1=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        DELETE_IND AS DELETE_IND,
        SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
        SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCURE_STORE_NBR AS PROCURE_STORE_NBR,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_STORE_VENDOR_DAY_0 
    WHERE
        store_nbr IN (
            2940, 2941, 2801, 2859, 2877
        ) 
        AND delete_ind <> 'X'""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_SKU_STORE_VENDOR_DAY_1")

# COMMAND ----------
# DBTITLE 1, SKU_STORE_VENDOR_DAY


spark.sql("""INSERT INTO SKU_STORE_VENDOR_DAY SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
VENDOR_ID AS VENDOR_ID,
DELETE_IND AS DELETE_IND,
SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
PROCURE_SITE_ID AS PROCURE_STORE_NBR,
LOAD_DT AS LOAD_DT FROM SQ_Shortcut_To_SKU_STORE_VENDOR_DAY_1""")