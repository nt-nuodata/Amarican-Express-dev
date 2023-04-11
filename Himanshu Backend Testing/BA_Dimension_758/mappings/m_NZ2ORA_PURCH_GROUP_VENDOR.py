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
# DBTITLE 1, PURCH_GROUP_VENDOR_0


df_0=spark.sql("""
    SELECT
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        VENDOR_ID AS VENDOR_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PURCH_GROUP_VENDOR""")

df_0.createOrReplaceTempView("PURCH_GROUP_VENDOR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PURCH_GROUP_VENDOR_1


df_1=spark.sql("""
    SELECT
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        VENDOR_ID AS VENDOR_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PURCH_GROUP_VENDOR_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PURCH_GROUP_VENDOR_1")

# COMMAND ----------
# DBTITLE 1, Exp_Conversion_2


df_2=spark.sql("""
    SELECT
        (CAST(PURCH_GROUP_ID AS DECIMAL (38,
        0))) AS PURCH_GROUP_ID,
        (CAST(VENDOR_ID AS DECIMAL (38,
        0))) AS VENDOR_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PURCH_GROUP_VENDOR_1""")

df_2.createOrReplaceTempView("Exp_Conversion_2")

# COMMAND ----------
# DBTITLE 1, PURCH_GROUP_VENDOR


spark.sql("""INSERT INTO PURCH_GROUP_VENDOR SELECT PURCH_GROUP_ID AS PURCH_GROUP_ID,
VENDOR_ID AS VENDOR_ID FROM Exp_Conversion_2""")