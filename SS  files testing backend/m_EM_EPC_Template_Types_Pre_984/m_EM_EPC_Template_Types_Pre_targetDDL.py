# Databricks notebook source
# COMMAND ----------

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.EM_EPC_TEMPLATE_TYPES_PRE(EM_AMS_TEMPLATE_TYPE_ID INT,
EM_SAP_TEMPLATE_TYPE_ID STRING,
EM_AMS_TEMPLATE_TYPE_NAME STRING,
EM_AMS_TEMPLATE_TYPE_DESC STRING) USING DELTA;