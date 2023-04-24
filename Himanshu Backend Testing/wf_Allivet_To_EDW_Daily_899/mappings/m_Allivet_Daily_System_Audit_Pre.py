# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Shortcut_to_Allivet_Daily_System_Audit_0


df_0 = spark.sql("""SELECT
  Operation AS Operation,
  CustomerCode AS CustomerCode,
  ExtraInfo AS ExtraInfo,
  ChangeDate AS ChangeDate,
  FromItemCode AS FromItemCode,
  ToItemCode AS ToItemCode,
  AllivetOrderCode AS AllivetOrderCode,
  LineNum AS LineNum,
  PrescriptionID AS PrescriptionID,
  Counter AS Counter
FROM
  Allivet_Daily_System_Audit""")

df_0.createOrReplaceTempView("Shortcut_to_Allivet_Daily_System_Audit_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Allivet_Daily_System_Audit_1


df_1 = spark.sql("""SELECT
  Operation AS Operation,
  CustomerCode AS CustomerCode,
  ExtraInfo AS ExtraInfo,
  ChangeDate AS ChangeDate,
  FromItemCode AS FromItemCode,
  ToItemCode AS ToItemCode,
  AllivetOrderCode AS AllivetOrderCode,
  LineNum AS LineNum,
  PrescriptionID AS PrescriptionID,
  Counter AS Counter,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Allivet_Daily_System_Audit_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Allivet_Daily_System_Audit_1")

# COMMAND ----------
# DBTITLE 1, Exp_LoadTstmp_2


df_2 = spark.sql("""SELECT
  Operation AS Operation,
  CustomerCode AS CustomerCode,
  ExtraInfo AS ExtraInfo,
  to_date(ChangeDate, 'MM/DD/YYYY HH12:MI:SSAM') AS o_ChangeDate,
  FromItemCode AS FromItemCode,
  ToItemCode AS ToItemCode,
  AllivetOrderCode AS AllivetOrderCode,
  LineNum AS LineNum,
  PrescriptionID AS PrescriptionID,
  Counter AS Counter,
  now() AS Load_tstmp,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Allivet_Daily_System_Audit_1""")

df_2.createOrReplaceTempView("Exp_LoadTstmp_2")

# COMMAND ----------
# DBTITLE 1, ALLIVET_DAILY_SYSTEM_AUDIT_PRE


spark.sql("""INSERT INTO
  ALLIVET_DAILY_SYSTEM_AUDIT_PRE
SELECT
  AllivetOrderCode AS ALLIVET_ORDER_CODE,
  Counter AS ALLIVET_ORDER_COUNTER,
  LineNum AS ALLIVET_ORDER_LINE_NUMBER,
  Operation AS OPERATION,
  CustomerCode AS CUSTOMER_CODE,
  ExtraInfo AS EXTRA_INFO,
  o_ChangeDate AS CHANGE_DATE,
  FromItemCode AS FROM_ITEM_CODE,
  ToItemCode AS TO_ITEM_CODE,
  PrescriptionID AS PRESCRIPTION_ID,
  Load_tstmp AS LOAD_TSTMP
FROM
  Exp_LoadTstmp_2""")