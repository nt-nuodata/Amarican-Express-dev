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
# DBTITLE 1, TP_SEX_0


df_0=spark.sql("""
    SELECT
        SEX_ID AS SEX_ID,
        SEX_DESC AS SEX_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TP_SEX""")

df_0.createOrReplaceTempView("TP_SEX_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TP_SEX_1


df_1=spark.sql("""
    SELECT
        SEX_ID AS SEX_ID,
        SEX_DESC AS SEX_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        TP_SEX_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_TP_SEX_1")

# COMMAND ----------
# DBTITLE 1, TP_SEX


spark.sql("""INSERT INTO TP_SEX SELECT SEX_ID AS SEX_ID,
SEX_DESC AS SEX_DESC FROM SQ_Shortcut_to_TP_SEX_1""")