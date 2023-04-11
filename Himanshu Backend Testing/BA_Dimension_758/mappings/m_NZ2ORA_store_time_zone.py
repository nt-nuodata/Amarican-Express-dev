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
# DBTITLE 1, STORE_TIME_ZONE_0


df_0=spark.sql("""
    SELECT
        SITE_NBR AS SITE_NBR,
        TIME_ZONE AS TIME_ZONE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STORE_TIME_ZONE""")

df_0.createOrReplaceTempView("STORE_TIME_ZONE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_STORE_TIME_ZONE_1


df_1=spark.sql("""
    SELECT
        SITE_NBR AS SITE_NBR,
        TIME_ZONE AS TIME_ZONE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        STORE_TIME_ZONE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_STORE_TIME_ZONE_1")

# COMMAND ----------
# DBTITLE 1, STORE_TIME_ZONE


spark.sql("""INSERT INTO STORE_TIME_ZONE SELECT SITE_NBR AS SITE_NBR,
TIME_ZONE AS TIME_ZONE FROM SQ_Shortcut_to_STORE_TIME_ZONE_1""")