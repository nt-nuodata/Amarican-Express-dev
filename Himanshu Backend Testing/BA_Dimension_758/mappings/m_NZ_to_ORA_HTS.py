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
# DBTITLE 1, HTS_0


df_0=spark.sql("""
    SELECT
        HTS_CODE_ID AS HTS_CODE_ID,
        HTS_CODE_DESC AS HTS_CODE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        HTS""")

df_0.createOrReplaceTempView("HTS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_HTS_1


df_1=spark.sql("""
    SELECT
        HTS_CODE_ID AS HTS_CODE_ID,
        HTS_CODE_DESC AS HTS_CODE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        HTS_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_HTS_1")

# COMMAND ----------
# DBTITLE 1, HTS


spark.sql("""INSERT INTO HTS SELECT HTS_CODE_ID AS HTS_CODE_ID,
HTS_CODE_DESC AS HTS_CODE_DESC FROM SQ_Shortcut_To_HTS_1""")