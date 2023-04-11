# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, PET_ALLERGIES_PRE_0


df_0=spark.sql("""
    SELECT
        ALLERGY_ID AS ALLERGY_ID,
        DESCRIPTION AS DESCRIPTION,
        IS_ACTIVE AS IS_ACTIVE,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PET_ALLERGIES_PRE""")

df_0.createOrReplaceTempView("PET_ALLERGIES_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_ALLERGIES_PRE_1


df_1=spark.sql("""
    SELECT
        ALLERGY_ID AS ALLERGY_ID,
        DESCRIPTION AS DESCRIPTION,
        IS_ACTIVE AS IS_ACTIVE,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PET_ALLERGIES_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PET_ALLERGIES_PRE_1")

# COMMAND ----------
# DBTITLE 1, PET_ALLERGIES_2


df_2=spark.sql("""
    SELECT
        PET_ALLERGY_ID AS PET_ALLERGY_ID,
        PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PET_ALLERGIES""")

df_2.createOrReplaceTempView("PET_ALLERGIES_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_ALLERGIES_3


df_3=spark.sql("""
    SELECT
        PET_ALLERGY_ID AS PET_ALLERGY_ID,
        PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PET_ALLERGIES_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PET_ALLERGIES_3")

# COMMAND ----------
# DBTITLE 1, jnr_Pre_DWTable_4


df_4=spark.sql("""
    SELECT
        MASTER.ALLERGY_ID AS ALLERGY_ID,
        MASTER.DESCRIPTION AS DESCRIPTION,
        MASTER.IS_ACTIVE AS IS_ACTIVE,
        DETAIL.PET_ALLERGY_ID AS PET_ALLERGY_ID,
        DETAIL.PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
        DETAIL.IS_ACTIVE AS IS_ACTIVE1,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PET_ALLERGIES_PRE_1 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_PET_ALLERGIES_3 DETAIL 
            ON MASTER.ALLERGY_ID = DETAIL.PET_ALLERGY_ID""")

df_4.createOrReplaceTempView("jnr_Pre_DWTable_4")

# COMMAND ----------
# DBTITLE 1, exp_FLAGS_5


df_5=spark.sql("""
    SELECT
        IFF(ISNULL(ALLERGY_ID),
        'INSERT',
        IFF(MD5((to_char(ALLERGY_ID) || ltrim(rtrim(DESCRIPTION)) || to_char(IS_ACTIVE))) != MD5((to_char(ALLERGY_ID) || ltrim(rtrim(PET_ALLERGY_DESC)) || to_char(IS_ACTIVE1))),
        'UPDATE',
        'REJECT')) AS LOAD_FLAG,
        ALLERGY_ID AS PET_ALLERGY_ID,
        DESCRIPTION AS PET_ALLERGY_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        current_timestamp AS UPDATE_TSTMP,
        current_timestamp AS LOAD_TSTMP,
        ALLERGY_ID AS PET_ALLERGY_ID1,
        DESCRIPTION AS PET_ALLERGY_DESC1,
        IS_ACTIVE1 AS IS_ACTIVE1,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        jnr_Pre_DWTable_4""")

df_5.createOrReplaceTempView("exp_FLAGS_5")

# COMMAND ----------
# DBTITLE 1, fil_FLAGS_6


df_6=spark.sql("""
    SELECT
        LOAD_FLAG AS LOAD_FLAG,
        PET_ALLERGY_ID AS PET_ALLERGY_ID,
        PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        exp_FLAGS_5 
    WHERE
        LOAD_FLAG IN (
            'INSERT', 'UPDATE'
        )""")

df_6.createOrReplaceTempView("fil_FLAGS_6")

# COMMAND ----------
# DBTITLE 1, upd_FLAG_7


df_7=spark.sql("""
    SELECT
        LOAD_FLAG AS LOAD_FLAG,
        PET_ALLERGY_ID AS PET_ALLERGY_ID,
        PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        fil_FLAGS_6""")

df_7.createOrReplaceTempView("upd_FLAG_7")

# COMMAND ----------
# DBTITLE 1, PET_ALLERGIES


spark.sql("""INSERT INTO PET_ALLERGIES SELECT PET_ALLERGY_ID AS PET_ALLERGY_ID,
PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
IS_ACTIVE AS IS_ACTIVE,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM upd_FLAG_7""")