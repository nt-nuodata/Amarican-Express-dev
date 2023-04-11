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
# DBTITLE 1, COUNTRY_0


df_0=spark.sql("""
    SELECT
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        COUNTRY""")

df_0.createOrReplaceTempView("COUNTRY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_COUNTRY_1


df_1=spark.sql("""
    SELECT
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        COUNTRY_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_COUNTRY_1")

# COMMAND ----------
# DBTITLE 1, COUNTRY


spark.sql("""INSERT INTO COUNTRY SELECT COUNTRY_CD AS COUNTRY_CD,
COUNTRY_NAME AS COUNTRY_NAME FROM SQ_Shortcut_to_COUNTRY_1""")