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
# DBTITLE 1, TP_BREED_0


df_0=spark.sql("""
    SELECT
        BREED_ID AS BREED_ID,
        BREED_DESC AS BREED_DESC,
        SIZE_ID AS SIZE_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TP_BREED""")

df_0.createOrReplaceTempView("TP_BREED_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TP_BREED_1


df_1=spark.sql("""
    SELECT
        BREED_ID AS BREED_ID,
        BREED_DESC AS BREED_DESC,
        SIZE_ID AS SIZE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        TP_BREED_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_TP_BREED_1")

# COMMAND ----------
# DBTITLE 1, TP_BREED


spark.sql("""INSERT INTO TP_BREED SELECT BREED_ID AS BREED_ID,
BREED_DESC AS BREED_DESC,
SIZE_ID AS SIZE_ID FROM SQ_Shortcut_to_TP_BREED_1""")