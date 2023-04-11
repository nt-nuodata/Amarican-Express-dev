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
# DBTITLE 1, TP_ROOM_TYPE_0


df_0=spark.sql("""
    SELECT
        ROOM_TYPE_ID AS ROOM_TYPE_ID,
        ROOM_TYPE_DESC AS ROOM_TYPE_DESC,
        ROOM_TYPE_ABBREV AS ROOM_TYPE_ABBREV,
        WARNING_ID AS WARNING_ID,
        ROOM_CAPACITY_AMT AS ROOM_CAPACITY_AMT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TP_ROOM_TYPE""")

df_0.createOrReplaceTempView("TP_ROOM_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TP_ROOM_TYPE_1


df_1=spark.sql("""
    SELECT
        ROOM_TYPE_ID AS ROOM_TYPE_ID,
        ROOM_TYPE_DESC AS ROOM_TYPE_DESC,
        ROOM_TYPE_ABBREV AS ROOM_TYPE_ABBREV,
        WARNING_ID AS WARNING_ID,
        ROOM_CAPACITY_AMT AS ROOM_CAPACITY_AMT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        TP_ROOM_TYPE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_TP_ROOM_TYPE_1")

# COMMAND ----------
# DBTITLE 1, TP_ROOM_TYPE


spark.sql("""INSERT INTO TP_ROOM_TYPE SELECT ROOM_TYPE_ID AS ROOM_TYPE_ID,
ROOM_TYPE_DESC AS ROOM_TYPE_DESC,
ROOM_TYPE_ABBREV AS ROOM_TYPE_ABBREV,
WARNING_ID AS WARNING_ID,
ROOM_CAPACITY_AMT AS ROOM_CAPACITY_AMT FROM SQ_Shortcut_to_TP_ROOM_TYPE_1""")