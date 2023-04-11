# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, T009B_Pre_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        PERIV AS PERIV,
        BDATJ AS BDATJ,
        BUMON AS BUMON,
        BUTAG AS BUTAG,
        POPER AS POPER,
        RELJR AS RELJR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T009B_Pre""")

df_0.createOrReplaceTempView("T009B_Pre_0")

# COMMAND ----------
# DBTITLE 1, DAYS_1


df_1=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
        HOLIDAY_FLAG AS HOLIDAY_FLAG,
        DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
        DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
        DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
        CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
        CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
        CAL_WK AS CAL_WK,
        CAL_WK_NBR AS CAL_WK_NBR,
        CAL_MO AS CAL_MO,
        CAL_MO_NBR AS CAL_MO_NBR,
        CAL_MO_NAME AS CAL_MO_NAME,
        CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
        CAL_QTR AS CAL_QTR,
        CAL_QTR_NBR AS CAL_QTR_NBR,
        CAL_HALF AS CAL_HALF,
        CAL_YR AS CAL_YR,
        FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
        FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_WK_NBR AS FISCAL_WK_NBR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        FISCAL_QTR AS FISCAL_QTR,
        FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
        FISCAL_HALF AS FISCAL_HALF,
        FISCAL_YR AS FISCAL_YR,
        LYR_WEEK_DT AS LYR_WEEK_DT,
        LWK_WEEK_DT AS LWK_WEEK_DT,
        WEEK_DT AS WEEK_DT,
        EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
        EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
        ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
        ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
        CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
        CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
        CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
        CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
        MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
        MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
        MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
        MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
        PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
        PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DAYS""")

df_1.createOrReplaceTempView("DAYS_1")

# COMMAND ----------
# DBTITLE 1, SQ_T009B_Pre_2


df_2=spark.sql("""
    WITH FISCALPERIOD_PRE (FISCAL_YR, FISCAL_MO, FISCAL_MO_NBR, FISCAL_MO_NAME, FISCAL_MO_NAME_ABBR, END_WK_DT, RNK) AS (SELECT
        FISCAL_YR,
        FISCAL_MO,
        FISCAL_MO_NBR,
        FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR,
        MAX(END_WK_DT) AS END_WK_DT,
        RANK() OVER (
    ORDER BY
        FISCAL_MO) AS RNK 
    FROM
        (SELECT
            BDATJ + RELJR AS FISCAL_YR,
            CAST((BDATJ + RELJR) AS VARCHAR (4)) + REPLICATE('0',
            2 - LEN(POPER)) + CAST(POPER AS VARCHAR (2)) AS FISCAL_MO,
            POPER AS FISCAL_MO_NBR,
            DATENAME(month,
            CAST('9999' + CASE 
                WHEN POPER + 1 <= 12 THEN REPLICATE('0',
                2 - LEN(POPER + 1)) + CAST(POPER + 1 AS VARCHAR (2)) 
                ELSE '01' 
            END + '01' AS DATETIME)) AS FISCAL_MO_NAME,
            SUBSTRING(DATENAME(month,
            CAST('9999' + CASE 
                WHEN POPER + 1 <= 12 THEN REPLICATE('0',
                2 - LEN(POPER + 1)) + CAST(POPER + 1 AS VARCHAR (2)) 
                ELSE '01' 
            END + '01' AS DATETIME)),
            1,
            3) AS FISCAL_MO_NAME_ABBR,
            CAST(CAST(BDATJ AS VARCHAR (4)) + (REPLICATE('0',
            2 - LEN(BUMON)) + CAST(BUMON AS VARCHAR (2))) + (REPLICATE('0',
            2 - LEN(BUTAG)) + CAST(BUTAG AS VARCHAR (2))) AS DATETIME) AS END_WK_DT 
        FROM
            T009B_PRE) T 
    GROUP BY
        FISCAL_YR,
        FISCAL_MO,
        FISCAL_MO_NBR,
        FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR) SELECT
            E.FISCAL_YR,
            E.FISCAL_MO,
            E.FISCAL_MO_NBR,
            E.FISCAL_MO_NAME,
            E.FISCAL_MO_NAME_ABBR,
            S.END_WK_DT + 7 AS START_WK_DT,
            E.END_WK_DT,
            monotonically_increasing_id() AS Monotonically_Increasing_Id 
        FROM
            FISCALPERIOD_PRE E,
            FISCALPERIOD_PRE S 
        WHERE
            E.RNK = S.RNK + 1 
        ORDER BY
            E.FISCAL_MO""")

df_2.createOrReplaceTempView("SQ_T009B_Pre_2")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_3


df_3=spark.sql("""
    SELECT
        FISCAL_YR AS FISCAL_YR,
        to_char(ADD_TO_DATE(current_timestamp,
        'YY',
        $$year),
        'YYYY') AS o_FISCAL_YR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        START_WK_DT AS START_WK_DT,
        END_WK_DT AS END_WK_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_T009B_Pre_2""")

df_3.createOrReplaceTempView("EXPTRANS_3")

# COMMAND ----------
# DBTITLE 1, FILTRANS_4


df_4=spark.sql("""
    SELECT
        FISCAL_YR AS FISCAL_YR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        START_WK_DT AS START_WK_DT,
        END_WK_DT AS END_WK_DT,
        o_FISCAL_YR AS o_FISCAL_YR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_3 
    WHERE
        FISCAL_YR = o_FISCAL_YR 
        OR FISCAL_YR = to_char(ADD_TO_DATE(current_timestamp, 'YY', $$year + 1), 'YYYY')""")

df_4.createOrReplaceTempView("FILTRANS_4")

# COMMAND ----------
# DBTITLE 1, EXPTRANS1_5


df_5=spark.sql("""
    SELECT
        FISCAL_YR AS FISCAL_YR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        START_WK_DT AS START_WK_DT,
        END_WK_DT AS END_WK_DT,
        IFF(to_char(to_date(to_char(START_WK_DT,
        'YYYYMMDD HH24:MI:SS'),
        'YYYYMMDD HH24:MI:SS'),
        'DAY') != 'Sunday',
        Error('Data quality Issue,the day should be Sunday'),
        to_char(to_date(to_char(START_WK_DT,
        'YYYYMMDD HH24:MI:SS'),
        'YYYYMMDD HH24:MI:SS'),
        'DAY')) AS Abort_start,
        IFF(to_char(to_date(to_char(END_WK_DT,
        'YYYYMMDD HH24:MI:SS'),
        'YYYYMMDD HH24:MI:SS'),
        'DAY') != 'Sunday',
        Error('Data quality Issue,the day should be Sunday'),
        to_char(to_date(to_char(END_WK_DT,
        'YYYYMMDD HH24:MI:SS'),
        'YYYYMMDD HH24:MI:SS'),
        'DAY')) AS Abort_end,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FILTRANS_4""")

df_5.createOrReplaceTempView("EXPTRANS1_5")

# COMMAND ----------
# DBTITLE 1, FiscalPeriod


spark.sql("""INSERT INTO FiscalPeriod SELECT FISCAL_YR AS FiscalYr,
FISCAL_MO AS FiscalMo,
FISCAL_MO_NBR AS FiscalMoNbr,
FISCAL_MO_NAME AS FiscalMoName,
FISCAL_MO_NAME_ABBR AS FiscalMoNameAbbr,
START_WK_DT AS StartWkDt,
END_WK_DT AS EndWkDt FROM EXPTRANS1_5""")