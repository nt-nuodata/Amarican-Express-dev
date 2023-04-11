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
# DBTITLE 1, DISTRICT_0


df_0=spark.sql("""
    SELECT
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        DISTRICT_SITE_LOGIN_ID AS DISTRICT_SITE_LOGIN_ID,
        DISTRICT_SALON_LOGIN_ID AS DISTRICT_SALON_LOGIN_ID,
        DISTRICT_HOTEL_LOGIN_ID AS DISTRICT_HOTEL_LOGIN_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DISTRICT""")

df_0.createOrReplaceTempView("DISTRICT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DISTRICT_1


df_1=spark.sql("""
    SELECT
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        DISTRICT_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_DISTRICT_1")

# COMMAND ----------
# DBTITLE 1, DISTRICT_Ora


spark.sql("""INSERT INTO DISTRICT_Ora SELECT DISTRICT_ID AS DISTRICT_ID,
DISTRICT_DESC AS DISTRICT_DESC FROM SQ_Shortcut_to_DISTRICT_1""")