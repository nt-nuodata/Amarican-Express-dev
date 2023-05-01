# Databricks notebook source
# COMMAND ----------

CREATE DATABASE IF NOT EXISTS DELTA_TRAINING;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.EPC_TemplateTypes(TemplateTypeId INT,
TemplateTypeSAPId STRING,
TemplateTypeName STRING,
TemplateTypeDescription STRING) USING DELTA;