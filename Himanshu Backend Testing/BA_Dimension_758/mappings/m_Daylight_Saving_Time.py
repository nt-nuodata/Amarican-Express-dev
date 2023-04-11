# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Days_Pre2_0


df_0=spark.sql("""
    SELECT
        DayDt AS DayDt,
        BusinessDayFlag AS BusinessDayFlag,
        DayOfWkName AS DayOfWkName,
        DayOfWkNameAbbr AS DayOfWkNameAbbr,
        DayOfWkNbr AS DayOfWkNbr,
        CalDayOfMoNbr AS CalDayOfMoNbr,
        CalDayOfYrNbr AS CalDayOfYrNbr,
        CalWk AS CalWk,
        CalWkNbr AS CalWkNbr,
        CalMo AS CalMo,
        CalMoNbr AS CalMoNbr,
        CalMoName AS CalMoName,
        CalMoNameAbbr AS CalMoNameAbbr,
        CalQtr AS CalQtr,
        CalQtrNbr AS CalQtrNbr,
        CalHalf AS CalHalf,
        CalYr AS CalYr,
        LyrWeekDt AS LyrWeekDt,
        LwkWeekDt AS LwkWeekDt,
        WeekDt AS WeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Days_Pre2""")

df_0.createOrReplaceTempView("Days_Pre2_0")

# COMMAND ----------
# DBTITLE 1, SQ_Days_Pre2_1


df_1=spark.sql("""
    SELECT
        CALYR,
        MIN(DAYDT) AS START_DAY_DT,
        MAX(DAYDT) - 1 AS END_DAY_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            CalYr,
            DayDt,
            CASE 
                WHEN CALMONBR = 3 
                AND DAYOFWKNBR = 7 
                AND DENSE_RANK() OVER (PARTITION 
            BY
                CALMO,
                DAYOFWKNBR 
            ORDER BY
                WEEKDT) = 2 THEN 'S' 
                WHEN CALMONBR = 11 
                AND DAYOFWKNBR = 7 
                AND DENSE_RANK() OVER (PARTITION 
            BY
                CALMO,
                DAYOFWKNBR 
            ORDER BY
                WEEKDT) = 1 THEN 'E' 
                ELSE 'D' 
            END AS IND 
        FROM
            Days_Pre2) T 
    WHERE
        IND <> 'D' 
    GROUP BY
        CALYR""")

df_1.createOrReplaceTempView("SQ_Days_Pre2_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


df_2=spark.sql("""
    SELECT
        CalYr AS CalYr,
        to_char(ADD_TO_DATE(current_timestamp,
        'YY',
        $$year),
        'YYYY') AS O_Calyr,
        StartDayDt AS StartDayDt,
        EndDayDt AS EndDayDt,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Days_Pre2_1""")

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, FILTRANS_3


df_3=spark.sql("""
    SELECT
        CalYr AS CalYr,
        StartDayDt AS StartDayDt,
        EndDayDt AS EndDayDt,
        O_Calyr AS O_Calyr,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_2 
    WHERE
        CalYr = O_Calyr 
        OR CalYr = to_char(ADD_TO_DATE(current_timestamp, 'YY', $$year + 1), 'YYYY')""")

df_3.createOrReplaceTempView("FILTRANS_3")

# COMMAND ----------
# DBTITLE 1, Daylight_Saving_Time


spark.sql("""INSERT INTO Daylight_Saving_Time SELECT CalYr AS CalYr,
StartDayDt AS StartDayDt,
EndDayDt AS EndDayDt FROM FILTRANS_3""")