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
# DBTITLE 1, LOCATION_0


df_0=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        COMPANY_DESC AS COMPANY_DESC,
        COMPANY_ID AS COMPANY_ID,
        DATE_CLOSED AS DATE_CLOSED,
        DATE_OPEN AS DATE_OPEN,
        DATE_LOC_ADDED AS DATE_LOC_ADDED,
        DATE_LOC_DELETED AS DATE_LOC_DELETED,
        DATE_LOC_REFRESHED AS DATE_LOC_REFRESHED,
        DISTRICT_DESC AS DISTRICT_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        REGION_DESC AS REGION_DESC,
        REGION_ID AS REGION_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        STORE_CTRY AS STORE_CTRY,
        STORE_NAME AS STORE_NAME,
        STORE_NBR AS STORE_NBR,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        STORE_STATE_ABBR AS STORE_STATE_ABBR,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        EQUINE_MERCH AS EQUINE_MERCH,
        DATE_GR_OPEN AS DATE_GR_OPEN,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        TP_START_DT AS TP_START_DT,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        SITE_POSTAL_CD AS SITE_POSTAL_CD,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LOCATION""")

df_0.createOrReplaceTempView("LOCATION_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_LOCATION_1


df_1=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        COMPANY_DESC AS COMPANY_DESC,
        COMPANY_ID AS COMPANY_ID,
        DATE_CLOSED AS DATE_CLOSED,
        DATE_OPEN AS DATE_OPEN,
        DATE_LOC_ADDED AS DATE_LOC_ADDED,
        DATE_LOC_DELETED AS DATE_LOC_DELETED,
        DATE_LOC_REFRESHED AS DATE_LOC_REFRESHED,
        DISTRICT_DESC AS DISTRICT_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        REGION_DESC AS REGION_DESC,
        REGION_ID AS REGION_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        STORE_CTRY AS STORE_CTRY,
        STORE_NAME AS STORE_NAME,
        STORE_NBR AS STORE_NBR,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        STORE_STATE_ABBR AS STORE_STATE_ABBR,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        EQUINE_MERCH AS EQUINE_MERCH,
        DATE_GR_OPEN AS DATE_GR_OPEN,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        TP_START_DT AS TP_START_DT,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        SITE_POSTAL_CD AS SITE_POSTAL_CD,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LOCATION_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_LOCATION_1")

# COMMAND ----------
# DBTITLE 1, LOCATION


spark.sql("""INSERT INTO LOCATION SELECT LOCATION_ID AS LOCATION_ID,
COMPANY_DESC AS COMPANY_DESC,
COMPANY_ID AS COMPANY_ID,
DATE_CLOSED AS DATE_CLOSED,
DATE_OPEN AS DATE_OPEN,
DATE_LOC_ADDED AS DATE_LOC_ADDED,
DATE_LOC_DELETED AS DATE_LOC_DELETED,
DATE_LOC_REFRESHED AS DATE_LOC_REFRESHED,
DISTRICT_DESC AS DISTRICT_DESC,
DISTRICT_ID AS DISTRICT_ID,
PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
PRICE_ZONE_ID AS PRICE_ZONE_ID,
REGION_DESC AS REGION_DESC,
REGION_ID AS REGION_ID,
REPL_DC_NBR AS REPL_DC_NBR,
REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
STORE_CTRY AS STORE_CTRY,
STORE_NAME AS STORE_NAME,
STORE_NBR AS STORE_NBR,
STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
STORE_STATE_ABBR AS STORE_STATE_ABBR,
STORE_TYPE_DESC AS STORE_TYPE_DESC,
STORE_TYPE_ID AS STORE_TYPE_ID,
EQUINE_MERCH AS EQUINE_MERCH,
DATE_GR_OPEN AS DATE_GR_OPEN,
SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
BP_COMPANY_NBR AS BP_COMPANY_NBR,
BP_GL_ACCT AS BP_GL_ACCT,
TP_LOC_FLAG AS TP_LOC_FLAG,
TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
TP_START_DT AS TP_START_DT,
SITE_ADDRESS AS SITE_ADDRESS,
SITE_CITY AS SITE_CITY,
SITE_POSTAL_CD AS SITE_POSTAL_CD,
SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO FROM SQ_Shortcut_To_LOCATION_1""")