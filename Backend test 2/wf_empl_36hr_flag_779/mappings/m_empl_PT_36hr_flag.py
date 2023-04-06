# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, EMPLOYEE_PROFILE_DAY_PSOFT_0


df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        EMPLOYEE_ID AS EMPLOYEE_ID,
        EMPL_FIRST_NAME AS EMPL_FIRST_NAME,
        EMPL_MIDDLE_NAME AS EMPL_MIDDLE_NAME,
        EMPL_LAST_NAME AS EMPL_LAST_NAME,
        EMPL_BIRTH_DT AS EMPL_BIRTH_DT,
        GENDER_CD AS GENDER_CD,
        PS_MARITAL_STATUS_CD AS PS_MARITAL_STATUS_CD,
        ETHNIC_GROUP_ID AS ETHNIC_GROUP_ID,
        EMPL_ADDR_1 AS EMPL_ADDR_1,
        EMPL_ADDR_2 AS EMPL_ADDR_2,
        EMPL_CITY AS EMPL_CITY,
        EMPL_STATE AS EMPL_STATE,
        EMPL_PROVINCE AS EMPL_PROVINCE,
        EMPL_ZIPCODE AS EMPL_ZIPCODE,
        COUNTRY_CD AS COUNTRY_CD,
        EMPL_HOME_PHONE AS EMPL_HOME_PHONE,
        EMPL_EMAIL_ADDR AS EMPL_EMAIL_ADDR,
        EMPL_LOGIN_ID AS EMPL_LOGIN_ID,
        BADGE_NBR AS BADGE_NBR,
        EMPL_STATUS_CD AS EMPL_STATUS_CD,
        STATUS_CHG_DT AS STATUS_CHG_DT,
        FULLPT_FLAG AS FULLPT_FLAG,
        FULLPT_CHG_DT AS FULLPT_CHG_DT,
        EMPL_TYPE_CD AS EMPL_TYPE_CD,
        PS_REG_TEMP_CD AS PS_REG_TEMP_CD,
        EMPL_CATEGORY_CD AS EMPL_CATEGORY_CD,
        EMPL_GROUP_CD AS EMPL_GROUP_CD,
        EMPL_SUBGROUP_CD AS EMPL_SUBGROUP_CD,
        EMPL_HIRE_DT AS EMPL_HIRE_DT,
        EMPL_REHIRE_DT AS EMPL_REHIRE_DT,
        EMPL_TERM_DT AS EMPL_TERM_DT,
        TERM_REASON_CD AS TERM_REASON_CD,
        EMPL_SENORITY_DT AS EMPL_SENORITY_DT,
        PS_ACTION_DT AS PS_ACTION_DT,
        PS_ACTION_CD AS PS_ACTION_CD,
        PS_ACTION_REASON_CD AS PS_ACTION_REASON_CD,
        LOCATION_ID AS LOCATION_ID,
        LOCATION_CHG_DT AS LOCATION_CHG_DT,
        STORE_NBR AS STORE_NBR,
        STORE_DEPT_NBR AS STORE_DEPT_NBR,
        COMPANY_ID AS COMPANY_ID,
        PS_PERSONNEL_AREA_ID AS PS_PERSONNEL_AREA_ID,
        PS_PERSONNEL_SUBAREA_ID AS PS_PERSONNEL_SUBAREA_ID,
        PS_DEPT_CD AS PS_DEPT_CD,
        PS_DEPT_CHG_DT AS PS_DEPT_CHG_DT,
        PS_POSITION_ID AS PS_POSITION_ID,
        POSITION_CHG_DT AS POSITION_CHG_DT,
        PS_SUPERVISOR_ID AS PS_SUPERVISOR_ID,
        JOB_CODE AS JOB_CODE,
        JOB_CODE_CHG_DT AS JOB_CODE_CHG_DT,
        EMPL_JOB_ENTRY_DT AS EMPL_JOB_ENTRY_DT,
        PS_GRADE_ID AS PS_GRADE_ID,
        EMPL_STD_BONUS_PCT AS EMPL_STD_BONUS_PCT,
        EMPL_OVR_BONUS_PCT AS EMPL_OVR_BONUS_PCT,
        EMPL_RATING AS EMPL_RATING,
        PAY_RATE_CHG_DT AS PAY_RATE_CHG_DT,
        PS_PAYROLL_AREA_CD AS PS_PAYROLL_AREA_CD,
        PS_TAX_COMPANY_CD AS PS_TAX_COMPANY_CD,
        PS_COMP_FREQ_CD AS PS_COMP_FREQ_CD,
        COMP_RATE_AMT AS COMP_RATE_AMT,
        ANNUAL_RATE_LOC_AMT AS ANNUAL_RATE_LOC_AMT,
        HOURLY_RATE_LOC_AMT AS HOURLY_RATE_LOC_AMT,
        CURRENCY_ID AS CURRENCY_ID,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EMPLOYEE_PROFILE_DAY_PSOFT""")

df_0.createOrReplaceTempView("EMPLOYEE_PROFILE_DAY_PSOFT_0")

# COMMAND ----------
# DBTITLE 1, EMPL_EMPL_LOC_WK_1


df_1=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        EMPLOYEE_ID AS EMPLOYEE_ID,
        EARN_ID AS EARN_ID,
        STORE_DEPT_NBR AS STORE_DEPT_NBR,
        JOB_CODE AS JOB_CODE,
        FULLPT_FLAG AS FULLPT_FLAG,
        HOURS_WORKED AS HOURS_WORKED,
        EARNINGS_AMT AS EARNINGS_AMT,
        EARNINGS_LOC_AMT AS EARNINGS_LOC_AMT,
        PAY_FREQ_CD AS PAY_FREQ_CD,
        CURRENCY_NBR AS CURRENCY_NBR,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EMPL_EMPL_LOC_WK""")

df_1.createOrReplaceTempView("EMPL_EMPL_LOC_WK_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_EMPLOYEE_PROFILE_2


df_2=spark.sql("""
    SELECT
        DISTINCT ((CURRENT_DATE - (DATE_PART('DOW',
        CURRENT_DATE) - 1)) - 7) PERIOD_DT,
        A.EMPLOYEE_ID,
        B.LOCATION_ID,
        SUM(HOURS_WORKED) OVER (PARTITION 
    BY
        ((CURRENT_DATE - (DATE_PART('DOW',
        CURRENT_DATE) - 1)) - 7),
        B.LOCATION_ID,
        A.EMPLOYEE_ID ) HOURS_WORKED,
        SUM(HOURS_WORKED) OVER (PARTITION 
    BY
        ((CURRENT_DATE - (DATE_PART('DOW',
        CURRENT_DATE) - 1)) - 7),
        A.EMPLOYEE_ID ) TOT_HOURS_WORKED,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            * 
        FROM
            (SELECT
                * 
            FROM
                EMPLOYEE_PROFILE_DAY) T 
        WHERE
            DAY_DT = (
                (
                    CURRENT_DATE - (
                        DATE_PART('DOW', CURRENT_DATE) - 1
                    )
                ) - 7
            )
        ) A, (
            SELECT
                * 
            FROM
                EMPL_EMPL_LOC_WK
        ) B, EARNINGS_ID E 
    WHERE
        A.FULLPT_FLAG IN (
            'P'
        ) 
        AND A.EMPL_TYPE_CD IN (
            'H'
        ) 
        AND A.EMPL_STATUS_CD IN (
            'A'
        ) 
        AND A.JOB_CODE NOT IN (
            9000
        ) 
        AND A.EMPL_HIRE_DT < (
            (
                CURRENT_DATE - (
                    DATE_PART('DOW', CURRENT_DATE) - 1
                )
            ) - 91 - 7
        ) 
        AND A.FULLPT_CHG_DT < (
            (
                CURRENT_DATE - (
                    DATE_PART('DOW', CURRENT_DATE) - 1
                )
            ) - 91 - 7
        ) 
        AND A.STATUS_CHG_DT < (
            (
                CURRENT_DATE - (
                    DATE_PART('DOW', CURRENT_DATE) - 1
                )
            ) - 91 - 7
        ) 
        AND (
            A.LOCATION_CHG_DT < (
                (
                    CURRENT_DATE - (
                        DATE_PART('DOW', CURRENT_DATE) - 1
                    )
                ) - 91 - 7
            ) 
            OR A.LOCATION_CHG_DT IS NULL
        ) 
        AND A.EMPLOYEE_ID = B.EMPLOYEE_ID 
        AND B.EARN_ID = E.EARN_ID 
        AND B.EARN_ID NOT LIKE ('LJP%') 
        AND SUBSTR(B.EARN_ID, 4, 4) NOT IN (
            '2031', '2032', '2033', '31', '32', '33'
        ) 
        AND HOURS_WORKED > 0 
        AND B.WEEK_DT > (
            (
                CURRENT_DATE - (
                    DATE_PART('DOW', CURRENT_DATE) - 1
                )
            ) - 94 - 0
        ) 
        AND B.WEEK_DT <= (
            (
                CURRENT_DATE - (
                    DATE_PART('DOW', CURRENT_DATE) - 1
                )
            ) - 7
        )""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_EMPLOYEE_PROFILE_2")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_3


df_3=spark.sql("""
    SELECT
        PERIOD_DT AS PERIOD_DT,
        EMPLOYEE_ID AS EMPLOYEE_ID,
        LOCATION_ID AS LOCATION_ID,
        TO_DECIMAL(HOURS_WORKED / 13,
        4) AS HOURS_WORKED11,
        IFF(TOT_HOURS_WORKED / 13 > 36.00,
        1,
        0) AS Flag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_EMPLOYEE_PROFILE_2""")

df_3.createOrReplaceTempView("EXPTRANS_3")

# COMMAND ----------
# DBTITLE 1, EXP_ROUND_NET_SALES_COST_4


df_4=spark.sql("""
    SELECT
        ROUND(DECODE(TRUE,
        SUBSTR(HOURS_WORKED11,
        INSTR(HOURS_WORKED11,
        '.') + 3,
        1) = '5' 
        AND TO_DECIMAL(HOURS_WORKED11,
        4) > 0,
        TO_DECIMAL(HOURS_WORKED11,
        4) + 0.005,
        SUBSTR(HOURS_WORKED11,
        INSTR(HOURS_WORKED11,
        '.') + 3,
        1) = '5' 
        AND TO_DECIMAL(HOURS_WORKED11,
        4) < 0,
        TO_DECIMAL(HOURS_WORKED11,
        4) - 0.005,
        TO_DECIMAL(HOURS_WORKED11,
        4)),
        2) AS out_HOURS_WORKED_COST,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_3""")

df_4.createOrReplaceTempView("EXP_ROUND_NET_SALES_COST_4")

# COMMAND ----------
# DBTITLE 1, EARNINGS_ID_5


df_5=spark.sql("""
    SELECT
        EARN_ID AS EARN_ID,
        PS_TAX_COMPANY_CD AS PS_TAX_COMPANY_CD,
        PS_WAGE_TYPE_GID AS PS_WAGE_TYPE_GID,
        PS_WAGE_TYPE_CD AS PS_WAGE_TYPE_CD,
        PS_COUNTRY_GROUP_CD AS PS_COUNTRY_GROUP_CD,
        PS_WAGE_TYPE_DESC AS PS_WAGE_TYPE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EARNINGS_ID""")

df_5.createOrReplaceTempView("EARNINGS_ID_5")

# COMMAND ----------
# DBTITLE 1, EMPL_EMPL_LOC_WK_PSOFT_6


df_6=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        EMPLOYEE_ID AS EMPLOYEE_ID,
        EARN_ID AS EARN_ID,
        STORE_DEPT_NBR AS STORE_DEPT_NBR,
        JOB_CODE AS JOB_CODE,
        FULLPT_FLAG AS FULLPT_FLAG,
        HOURS_WORKED AS HOURS_WORKED,
        EARNINGS_AMT AS EARNINGS_AMT,
        EARNINGS_LOC_AMT AS EARNINGS_LOC_AMT,
        PAY_FREQ_CD AS PAY_FREQ_CD,
        CURRENCY_NBR AS CURRENCY_NBR,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EMPL_EMPL_LOC_WK_PSOFT""")

df_6.createOrReplaceTempView("EMPL_EMPL_LOC_WK_PSOFT_6")

# COMMAND ----------
# DBTITLE 1, EMPLOYEE_PROFILE_DAY_7


df_7=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        EMPLOYEE_ID AS EMPLOYEE_ID,
        EMPL_FIRST_NAME AS EMPL_FIRST_NAME,
        EMPL_MIDDLE_NAME AS EMPL_MIDDLE_NAME,
        EMPL_LAST_NAME AS EMPL_LAST_NAME,
        EMPL_BIRTH_DT AS EMPL_BIRTH_DT,
        GENDER_CD AS GENDER_CD,
        PS_MARITAL_STATUS_CD AS PS_MARITAL_STATUS_CD,
        ETHNIC_GROUP_ID AS ETHNIC_GROUP_ID,
        EMPL_ADDR_1 AS EMPL_ADDR_1,
        EMPL_ADDR_2 AS EMPL_ADDR_2,
        EMPL_CITY AS EMPL_CITY,
        EMPL_STATE AS EMPL_STATE,
        EMPL_PROVINCE AS EMPL_PROVINCE,
        EMPL_ZIPCODE AS EMPL_ZIPCODE,
        COUNTRY_CD AS COUNTRY_CD,
        EMPL_HOME_PHONE AS EMPL_HOME_PHONE,
        EMPL_EMAIL_ADDR AS EMPL_EMAIL_ADDR,
        EMPL_LOGIN_ID AS EMPL_LOGIN_ID,
        BADGE_NBR AS BADGE_NBR,
        EMPL_STATUS_CD AS EMPL_STATUS_CD,
        STATUS_CHG_DT AS STATUS_CHG_DT,
        FULLPT_FLAG AS FULLPT_FLAG,
        FULLPT_CHG_DT AS FULLPT_CHG_DT,
        EMPL_TYPE_CD AS EMPL_TYPE_CD,
        PS_REG_TEMP_CD AS PS_REG_TEMP_CD,
        EMPL_CATEGORY_CD AS EMPL_CATEGORY_CD,
        EMPL_GROUP_CD AS EMPL_GROUP_CD,
        EMPL_SUBGROUP_CD AS EMPL_SUBGROUP_CD,
        EMPL_HIRE_DT AS EMPL_HIRE_DT,
        EMPL_REHIRE_DT AS EMPL_REHIRE_DT,
        EMPL_TERM_DT AS EMPL_TERM_DT,
        TERM_REASON_CD AS TERM_REASON_CD,
        EMPL_SENORITY_DT AS EMPL_SENORITY_DT,
        PS_ACTION_DT AS PS_ACTION_DT,
        PS_ACTION_CD AS PS_ACTION_CD,
        PS_ACTION_REASON_CD AS PS_ACTION_REASON_CD,
        LOCATION_ID AS LOCATION_ID,
        LOCATION_CHG_DT AS LOCATION_CHG_DT,
        STORE_NBR AS STORE_NBR,
        STORE_DEPT_NBR AS STORE_DEPT_NBR,
        COMPANY_ID AS COMPANY_ID,
        PS_PERSONNEL_AREA_ID AS PS_PERSONNEL_AREA_ID,
        PS_PERSONNEL_SUBAREA_ID AS PS_PERSONNEL_SUBAREA_ID,
        PS_DEPT_CD AS PS_DEPT_CD,
        PS_DEPT_CHG_DT AS PS_DEPT_CHG_DT,
        PS_POSITION_ID AS PS_POSITION_ID,
        POSITION_CHG_DT AS POSITION_CHG_DT,
        PS_SUPERVISOR_ID AS PS_SUPERVISOR_ID,
        JOB_CODE AS JOB_CODE,
        JOB_CODE_CHG_DT AS JOB_CODE_CHG_DT,
        EMPL_JOB_ENTRY_DT AS EMPL_JOB_ENTRY_DT,
        PS_GRADE_ID AS PS_GRADE_ID,
        EMPL_STD_BONUS_PCT AS EMPL_STD_BONUS_PCT,
        EMPL_OVR_BONUS_PCT AS EMPL_OVR_BONUS_PCT,
        EMPL_RATING AS EMPL_RATING,
        PAY_RATE_CHG_DT AS PAY_RATE_CHG_DT,
        PS_PAYROLL_AREA_CD AS PS_PAYROLL_AREA_CD,
        PS_TAX_COMPANY_CD AS PS_TAX_COMPANY_CD,
        PS_COMP_FREQ_CD AS PS_COMP_FREQ_CD,
        COMP_RATE_AMT AS COMP_RATE_AMT,
        ANNUAL_RATE_LOC_AMT AS ANNUAL_RATE_LOC_AMT,
        HOURLY_RATE_LOC_AMT AS HOURLY_RATE_LOC_AMT,
        CURRENCY_ID AS CURRENCY_ID,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EMPLOYEE_PROFILE_DAY""")

df_7.createOrReplaceTempView("EMPLOYEE_PROFILE_DAY_7")

# COMMAND ----------
# DBTITLE 1, EMPL_PT_36HR_FLAG


spark.sql("""INSERT INTO EMPL_PT_36HR_FLAG SELECT E3.PERIOD_DT AS WEEK_DT,
E3.EMPLOYEE_ID AS EMPLOYEE_ID,
E3.LOCATION_ID AS LOCATION_ID,
ERNSC4.out_HOURS_WORKED_COST AS EMPL_13WK_AVG_HRS_WORKED,
E3.Flag AS FLAG,
null AS LOAD_TSTMP FROM EXP_ROUND_NET_SALES_COST_4 ERNSC4 INNER JOIN EXPTRANS_3 E3 ON ERNSC4.Monotonically_Increasing_Id = E3.Monotonically_Increasing_Id """)