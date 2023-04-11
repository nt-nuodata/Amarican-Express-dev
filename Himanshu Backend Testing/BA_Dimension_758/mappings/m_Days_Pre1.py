# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Days_Pre1_0


df_0=spark.sql("""
    SELECT
        DayDt AS DayDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Days_Pre1""")

df_0.createOrReplaceTempView("Days_Pre1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Days_Pre1_1


df_1=spark.sql("""DECLARE @StartDateTime DATETIME
DECLARE @EndDateTime DATETIME
 
SET @StartDateTime = (select  distinct dateadd(day,-6,startwkdt)  from fiscalperiod where fiscalmo=CONCAT(DATEPART(YEAR,dateadd(YY,$$year,GETDATE())),'01'))
SET @EndDateTime = (select  distinct endwkdt  from fiscalperiod where fiscalmo=CONCAT(DATEPART(YEAR,dateadd(YY,$$year+1,GETDATE())),'12'));
 
WITH DateRange(DateData) AS 
(
    SELECT @StartDateTime as Date
    UNION ALL
    SELECT DATEADD(d,1,DateData)
    FROM DateRange 
    WHERE DateData < @EndDateTime
)
SELECT DateData
FROM DateRange
OPTION (MAXRECURSION 0)""")

df_1.createOrReplaceTempView("SQ_Days_Pre1_1")

# COMMAND ----------
# DBTITLE 1, Days_Pre1


spark.sql("""INSERT INTO Days_Pre1 SELECT DayDt AS DayDt FROM SQ_Days_Pre1_1""")