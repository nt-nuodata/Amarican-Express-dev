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
# DBTITLE 1, PURCH_GROUP_0


df_0=spark.sql("""
    SELECT
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        DVL_MGR_ID AS DVL_MGR_ID,
        REPLEN_MGR_ID AS REPLEN_MGR_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PURCH_GROUP""")

df_0.createOrReplaceTempView("PURCH_GROUP_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PURCH_GROUP_1


df_1=spark.sql("""
    SELECT
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PURCH_GROUP_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PURCH_GROUP_1")

# COMMAND ----------
# DBTITLE 1, PURCH_GROUP


spark.sql("""INSERT INTO PURCH_GROUP SELECT PURCH_GROUP_ID AS PURCH_GROUP_ID,
PURCH_GROUP_NAME AS PURCH_GROUP_NAME FROM SQ_Shortcut_to_PURCH_GROUP_1""")