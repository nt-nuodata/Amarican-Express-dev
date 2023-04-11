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
# DBTITLE 1, TP_SPECIES_0


df_0=spark.sql("""
    SELECT
        SPECIES_ID AS SPECIES_ID,
        SPECIES_DESC AS SPECIES_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TP_SPECIES""")

df_0.createOrReplaceTempView("TP_SPECIES_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TP_SPECIES_1


df_1=spark.sql("""
    SELECT
        SPECIES_ID AS SPECIES_ID,
        SPECIES_DESC AS SPECIES_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        TP_SPECIES_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_TP_SPECIES_1")

# COMMAND ----------
# DBTITLE 1, TP_SPECIES


spark.sql("""INSERT INTO TP_SPECIES SELECT SPECIES_ID AS SPECIES_ID,
SPECIES_DESC AS SPECIES_DESC FROM SQ_Shortcut_to_TP_SPECIES_1""")