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
# DBTITLE 1, CR_CUST_ACCT_TYPE_0


df_0=spark.sql("""
    SELECT
        CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
        CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
        CUST_ACCT_RANK AS CUST_ACCT_RANK,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        CR_CUST_ACCT_TYPE""")

df_0.createOrReplaceTempView("CR_CUST_ACCT_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_CR_CUST_ACCT_TYPE_1


df_1=spark.sql("""
    SELECT
        CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
        CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
        CUST_ACCT_RANK AS CUST_ACCT_RANK,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        CR_CUST_ACCT_TYPE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_CR_CUST_ACCT_TYPE_1")

# COMMAND ----------
# DBTITLE 1, EXP_Transform_2


df_2=spark.sql("""
    SELECT
        CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
        CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
        CUST_ACCT_RANK AS CUST_ACCT_RANK,
        IFF(SALES_CUST_CAPTURE_CD = 'L',
        3,
        IFF(SALES_CUST_CAPTURE_CD = 'P',
        2,
        IFF(SALES_CUST_CAPTURE_CD = 'E',
        2,
        1))) AS CUST_CAPTURE_RANK,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_CR_CUST_ACCT_TYPE_1""")

df_2.createOrReplaceTempView("EXP_Transform_2")

# COMMAND ----------
# DBTITLE 1, CR_CUST_ACCT_TYPE


spark.sql("""INSERT INTO CR_CUST_ACCT_TYPE SELECT CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
CUST_ACCT_RANK AS CUST_ACCT_RANK,
CUST_CAPTURE_RANK AS CUST_CAPTURE_RANK FROM EXP_Transform_2""")