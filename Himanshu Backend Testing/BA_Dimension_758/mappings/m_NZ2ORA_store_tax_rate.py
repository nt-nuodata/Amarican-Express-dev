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
# DBTITLE 1, STORE_TAX_RATE_0


df_0=spark.sql("""
    SELECT
        SITE_NBR AS SITE_NBR,
        COUNTRY_CD AS COUNTRY_CD,
        JURISDICTION_TAX AS JURISDICTION_TAX,
        CITY_TAX AS CITY_TAX,
        COUNTY_TAX AS COUNTY_TAX,
        STATE_TAX AS STATE_TAX,
        PST AS PST,
        GST AS GST,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STORE_TAX_RATE""")

df_0.createOrReplaceTempView("STORE_TAX_RATE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_STORE_TAX_RATE_1


df_1=spark.sql("""
    SELECT
        SITE_NBR AS SITE_NBR,
        COUNTRY_CD AS COUNTRY_CD,
        JURISDICTION_TAX AS JURISDICTION_TAX,
        CITY_TAX AS CITY_TAX,
        COUNTY_TAX AS COUNTY_TAX,
        STATE_TAX AS STATE_TAX,
        PST AS PST,
        GST AS GST,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        STORE_TAX_RATE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_STORE_TAX_RATE_1")

# COMMAND ----------
# DBTITLE 1, STORE_TAX_RATE


spark.sql("""INSERT INTO STORE_TAX_RATE SELECT SITE_NBR AS SITE_NBR,
COUNTRY_CD AS COUNTRY_CD,
JURISDICTION_TAX AS JURISDICTION_TAX,
CITY_TAX AS CITY_TAX,
COUNTY_TAX AS COUNTY_TAX,
STATE_TAX AS STATE_TAX,
PST AS PST,
GST AS GST FROM SQ_Shortcut_to_STORE_TAX_RATE_1""")