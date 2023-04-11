# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, PET_BREED_PRE_0


df_0=spark.sql("""
    SELECT
        BREED_ID AS BREED_ID,
        DESCRIPTION AS DESCRIPTION,
        IS_AGGRESSIVE AS IS_AGGRESSIVE,
        SPECIES_ID AS SPECIES_ID,
        SPECIES_NAME AS SPECIES_NAME,
        IS_ACTIVE AS IS_ACTIVE,
        IS_FIRST_PARTY AS IS_FIRST_PARTY,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PET_BREED_PRE""")

df_0.createOrReplaceTempView("PET_BREED_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_BREED_PRE_1


df_1=spark.sql("""
    SELECT
        BREED_ID AS BREED_ID,
        DESCRIPTION AS DESCRIPTION,
        IS_AGGRESSIVE AS IS_AGGRESSIVE,
        SPECIES_ID AS SPECIES_ID,
        SPECIES_NAME AS SPECIES_NAME,
        IS_ACTIVE AS IS_ACTIVE,
        IS_FIRST_PARTY AS IS_FIRST_PARTY,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PET_BREED_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PET_BREED_PRE_1")

# COMMAND ----------
# DBTITLE 1, PET_BREED_2


df_2=spark.sql("""
    SELECT
        PET_BREED_ID AS PET_BREED_ID,
        PET_BREED_DESC AS PET_BREED_DESC,
        IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
        PET_SPECIES_ID AS PET_SPECIES_ID,
        PET_SPECIES_NAME AS PET_SPECIES_NAME,
        IS_ACTIVE AS IS_ACTIVE,
        IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PET_BREED""")

df_2.createOrReplaceTempView("PET_BREED_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_BREED_3


df_3=spark.sql("""
    SELECT
        PET_BREED_ID AS PET_BREED_ID,
        PET_BREED_DESC AS PET_BREED_DESC,
        IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
        PET_SPECIES_ID AS PET_SPECIES_ID,
        PET_SPECIES_NAME AS PET_SPECIES_NAME,
        IS_ACTIVE AS IS_ACTIVE,
        IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PET_BREED_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PET_BREED_3")

# COMMAND ----------
# DBTITLE 1, jnr_Pre_DWTable_4


df_4=spark.sql("""
    SELECT
        MASTER.BREED_ID AS BREED_ID,
        MASTER.DESCRIPTION AS DESCRIPTION,
        MASTER.IS_AGGRESSIVE AS IS_AGGRESSIVE,
        MASTER.SPECIES_ID AS SPECIES_ID,
        MASTER.SPECIES_NAME AS SPECIES_NAME,
        MASTER.IS_ACTIVE AS IS_ACTIVE,
        MASTER.IS_FIRST_PARTY AS IS_FIRST_PARTY,
        DETAIL.PET_BREED_ID AS PET_BREED_ID,
        DETAIL.PET_BREED_DESC AS PET_BREED_DESC,
        DETAIL.IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
        DETAIL.PET_SPECIES_ID AS PET_SPECIES_ID,
        DETAIL.PET_SPECIES_NAME AS PET_SPECIES_NAME,
        DETAIL.IS_ACTIVE AS IS_ACTIVE1,
        DETAIL.IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PET_BREED_PRE_1 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_PET_BREED_3 DETAIL 
            ON MASTER.BREED_ID = DETAIL.PET_BREED_ID""")

df_4.createOrReplaceTempView("jnr_Pre_DWTable_4")

# COMMAND ----------
# DBTITLE 1, exp_FLAGS_5


df_5=spark.sql("""
    SELECT
        current_timestamp AS UPDATE_TSTMP,
        current_timestamp AS LOAD_TSTMP,
        BREED_ID AS BREED_ID,
        DESCRIPTION AS DESCRIPTION,
        IS_AGGRESSIVE AS IS_AGGRESSIVE,
        SPECIES_ID AS SPECIES_ID,
        SPECIES_NAME AS SPECIES_NAME,
        IS_ACTIVE AS IS_ACTIVE,
        IFF(upper(LTRIM(RTRIM(IS_FIRST_PARTY))) = 'TRUE',
        1,
        0) AS IS_FIRST_PARTY_FLAG,
        IFF(ISNULL(PET_BREED_ID),
        'INSERT',
        IFF(MD5(to_char(BREED_ID) || ltrim(rtrim(DESCRIPTION)) || to_char(IS_AGGRESSIVE) || to_char(SPECIES_ID) || ltrim(rtrim(SPECIES_NAME)) || to_char(IS_ACTIVE) || to_char(IFF(upper(LTRIM(RTRIM(IS_FIRST_PARTY))) = 'TRUE',
        1,
        0))) != MD5(to_char(PET_BREED_ID) || ltrim(rtrim(PET_BREED_DESC)) || to_char(IS_AGGRESSIVE_FLAG) || to_char(PET_SPECIES_ID) || ltrim(rtrim(PET_SPECIES_NAME)) || to_char(IS_ACTIVE1) || to_char(IS_FIRST_PARTY_FLAG)),
        'UPDATE',
        'REJECT')) AS LOAD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        jnr_Pre_DWTable_4""")

df_5.createOrReplaceTempView("exp_FLAGS_5")

# COMMAND ----------
# DBTITLE 1, fil_FLAGS_6


df_6=spark.sql("""
    SELECT
        LOAD_FLAG AS LOAD_FLAG,
        PET_BREED_ID AS PET_BREED_ID,
        PET_BREED_DESC AS PET_BREED_DESC,
        IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
        PET_SPECIES_ID AS PET_SPECIES_ID,
        PET_SPECIES_NAME AS PET_SPECIES_NAME,
        IS_ACTIVE AS IS_ACTIVE,
        IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
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
        PET_BREED_ID AS PET_BREED_ID,
        PET_BREED_DESC AS PET_BREED_DESC,
        IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
        PET_SPECIES_ID AS PET_SPECIES_ID,
        PET_SPECIES_NAME AS PET_SPECIES_NAME,
        IS_ACTIVE AS IS_ACTIVE,
        IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        fil_FLAGS_6""")

df_7.createOrReplaceTempView("upd_FLAG_7")

# COMMAND ----------
# DBTITLE 1, PET_BREED


spark.sql("""INSERT INTO PET_BREED SELECT PET_BREED_ID AS PET_BREED_ID,
PET_BREED_DESC AS PET_BREED_DESC,
IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
PET_SPECIES_ID AS PET_SPECIES_ID,
PET_SPECIES_NAME AS PET_SPECIES_NAME,
IS_ACTIVE AS IS_ACTIVE,
IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM upd_FLAG_7""")