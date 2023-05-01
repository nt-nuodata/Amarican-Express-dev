# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Shortcut_to_EPC_TemplateTypes_0


df_0 = spark.sql("""SELECT
  TemplateTypeId AS TemplateTypeId,
  TemplateTypeSAPId AS TemplateTypeSAPId,
  TemplateTypeName AS TemplateTypeName,
  TemplateTypeDescription AS TemplateTypeDescription
FROM
  EPC_TemplateTypes""")

df_0.createOrReplaceTempView("Shortcut_to_EPC_TemplateTypes_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_EPC_TemplateTypes_1


df_1 = spark.sql("""SELECT
  TemplateTypeId AS TemplateTypeId,
  TemplateTypeSAPId AS TemplateTypeSAPId,
  TemplateTypeName AS TemplateTypeName,
  TemplateTypeDescription AS TemplateTypeDescription,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_EPC_TemplateTypes_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_EPC_TemplateTypes_1")

# COMMAND ----------
# DBTITLE 1, EM_EPC_TEMPLATE_TYPES_PRE


spark.sql("""INSERT INTO
  EM_EPC_TEMPLATE_TYPES_PRE
SELECT
  TemplateTypeId AS EM_AMS_TEMPLATE_TYPE_ID,
  TemplateTypeSAPId AS EM_SAP_TEMPLATE_TYPE_ID,
  TemplateTypeName AS EM_AMS_TEMPLATE_TYPE_NAME,
  TemplateTypeDescription AS EM_AMS_TEMPLATE_TYPE_DESC
FROM
  SQ_Shortcut_to_EPC_TemplateTypes_1""")