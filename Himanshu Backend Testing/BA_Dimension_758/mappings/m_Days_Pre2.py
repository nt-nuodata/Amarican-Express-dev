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
# DBTITLE 1, FiscalPeriod_1


df_1=spark.sql("""
    SELECT
        FiscalYr AS FiscalYr,
        FiscalMo AS FiscalMo,
        FiscalMoNbr AS FiscalMoNbr,
        FiscalMoName AS FiscalMoName,
        FiscalMoNameAbbr AS FiscalMoNameAbbr,
        StartWkDt AS StartWkDt,
        EndWkDt AS EndWkDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        FiscalPeriod""")

df_1.createOrReplaceTempView("FiscalPeriod_1")

# COMMAND ----------
# DBTITLE 1, SQ_Days_Pre1_2


df_2=spark.sql("""select * from (SELECT DAYDT,
       CASE WHEN DATEPART (DW, DAYDT) IN (1, 7)  -- SATURDAY AND SUNDAY
                 THEN 'N' 
            ELSE 'Y' 
       END AS BUSINESS_DAY_FLAG,
       DATENAME (WEEKDAY, DAYDT) AS DAY_OF_WK_NAME,
       SUBSTRING (DATENAME (WEEKDAY, DAYDT), 1, 3) AS DAY_OF_WK_NAME_ABBR,
       CASE WHEN DATEPART (DW, DAYDT) - 1 = 0  -- SUNDAY
                 THEN 7
            ELSE DATEPART (DW, DAYDT) - 1
       END AS DAY_OF_WK_NBR,
       DATENAME (DAY, DAYDT) AS CAL_DAY_OF_MO_NBR,
       DATENAME (DAYOFYEAR, DAYDT) AS CAL_DAY_OF_YR_NBR,
       CAST (DATEPART (YY, DAYDT + 7 - (DATEPART (DW, DAYDT - 1))) AS VARCHAR (4))
       + REPLICATE ('0',2 - LEN(DENSE_RANK () OVER (PARTITION BY 
                                DATEPART(YY,DAYDT + 7 - (DATEPART(DW, DAYDT - 1)))
                                ORDER BY DAYDT + 7 - (DATEPART(DW, DAYDT - 1)))))
       + CAST (DENSE_RANK () OVER (PARTITION BY 
               DATEPART (YY, DAYDT + 7 - (DATEPART (DW, DAYDT - 1)))
               ORDER BY DAYDT + 7 - (DATEPART (DW, DAYDT - 1))) AS VARCHAR (2))
         AS CAL_WK,
       DENSE_RANK () OVER (PARTITION BY 
       DATEPART (YY, DAYDT + 7 - (DATEPART (DW, DAYDT - 1)))
       ORDER BY DAYDT + 7 - (DATEPART (DW, DAYDT - 1)))
         AS CAL_WK_NBR,
       CAST (DATEPART (YY, DAYDT) AS VARCHAR (4))
       + REPLICATE ('0', 2 - LEN (DATEPART (MM, DAYDT)))
       + CAST (DATEPART (MM, DAYDT) AS VARCHAR (2))
         AS CAL_MO,
       DATEPART (MM, DAYDT) AS CAL_MO_NBR,
       DATENAME (MM, DAYDT) AS CAL_MO_NAME,
       SUBSTRING (DATENAME (MM, DAYDT), 1, 3) AS CAL_MO_NAME_ABBR,
       CAST (DATEPART (YY, DAYDT) AS VARCHAR (4))
       + REPLICATE ('0', 2 - LEN (DATEPART (QUARTER, DAYDT)))
       + CAST (DATEPART (QUARTER, DAYDT) AS VARCHAR (2))
          AS CAL_QTR,
       DATEPART (QUARTER, DAYDT) AS CAL_QTR_NBR,
       CAST (DATEPART (YY, DAYDT) AS VARCHAR (4))
       + (CASE WHEN DATEPART (QUARTER, DAYDT) <= 2 THEN '01' ELSE '02' END)
          AS CAL_HALF,
       DATEPART (YY, DAYDT) AS CAL_YR,
       (DAYDT + 7 - (DATEPART (DW, DAYDT - 1))) - (52 * 7) AS LYR_WEEK_DT,
       DAYDT - (DATEPART (DW, DAYDT - 1)) AS LWK_WEEK_DT,
       DAYDT + 7 - (DATEPART (DW, DAYDT - 1)) AS WEEK_DT
  FROM DAYS_PRE1 )a where WEEK_DT between (select STARTWKDT from fiscalperiod where fiscalmo=CONCAT(DATEPART(YEAR,dateadd(YY,$$year,GETDATE())),'01')) and (select ENDWKDT from fiscalperiod where fiscalmo=CONCAT(DATEPART(YEAR,dateadd(YY,$$year+1,GETDATE())),'12')) order by DAYDT""")

df_2.createOrReplaceTempView("SQ_Days_Pre1_2")

# COMMAND ----------
# DBTITLE 1, Days_Pre2


spark.sql("""INSERT INTO Days_Pre2 SELECT DAYDT AS DayDt,
BUSINESS_DAY_FLAG AS BusinessDayFlag,
DAY_OF_WK_NAME AS DayOfWkName,
DAY_OF_WK_NAME_ABBR AS DayOfWkNameAbbr,
DAY_OF_WK_NBR AS DayOfWkNbr,
CAL_DAY_OF_MO_NBR AS CalDayOfMoNbr,
CAL_DAY_OF_YR_NBR AS CalDayOfYrNbr,
CAL_WK AS CalWk,
CAL_WK_NBR AS CalWkNbr,
CAL_MO AS CalMo,
CAL_MO_NBR AS CalMoNbr,
CAL_MO_NAME AS CalMoName,
CAL_MO_NAME_ABBR AS CalMoNameAbbr,
CAL_QTR AS CalQtr,
CAL_QTR_NBR AS CalQtrNbr,
CAL_HALF AS CalHalf,
CAL_YR AS CalYr,
LYR_WEEK_DT AS LyrWeekDt,
LWK_WEEK_DT AS LwkWeekDt,
WEEK_DT AS WeekDt FROM SQ_Days_Pre1_2""")