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
# DBTITLE 1, SAP_CATEGORY_0


df_0=spark.sql("""
    SELECT
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        GL_CATEGORY_CD AS GL_CATEGORY_CD,
        SAP_PRICING_CATEGORY_ID AS SAP_PRICING_CATEGORY_ID,
        UPD_TSTMP AS UPD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_CATEGORY""")

df_0.createOrReplaceTempView("SAP_CATEGORY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SAP_CATEGORY_1


df_1=spark.sql("""
    SELECT
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        GL_CATEGORY_CD AS GL_CATEGORY_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_CATEGORY_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SAP_CATEGORY_1")

# COMMAND ----------
# DBTITLE 1, SAP_CATEGORY_Ora


spark.sql("""INSERT INTO SAP_CATEGORY_Ora SELECT SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
SAP_CLASS_ID AS SAP_CLASS_ID,
GL_CATEGORY_CD AS GL_CATEGORY_CD FROM SQ_Shortcut_to_SAP_CATEGORY_1""")