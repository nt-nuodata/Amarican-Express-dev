# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ALLIVET_ORDER_DAY_0


df_0=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        ALLIVET_ORDER_LN_NBR AS ALLIVET_ORDER_LN_NBR,
        ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
        PETSMART_ORDER_DT AS PETSMART_ORDER_DT,
        ORDER_STATUS AS ORDER_STATUS,
        PRODUCT_ID AS PRODUCT_ID,
        PETSMART_ORDER_NBR AS PETSMART_ORDER_NBR,
        PETSMART_SKU_NBR AS PETSMART_SKU_NBR,
        ALLIVET_SKU_NBR AS ALLIVET_SKU_NBR,
        SUB_TOTAL_AMT AS SUB_TOTAL_AMT,
        FREIGHT_COST AS FREIGHT_COST,
        TOTAL_AMT AS TOTAL_AMT,
        SHIP_METHOD_CD AS SHIP_METHOD_CD,
        ORDER_VOIDED_FLAG AS ORDER_VOIDED_FLAG,
        ORDER_ONHOLD_FLAG AS ORDER_ONHOLD_FLAG,
        ORDER_CREATED_DT AS ORDER_CREATED_DT,
        ORDER_MODIFIED_DT AS ORDER_MODIFIED_DT,
        SHIPPED_DT AS SHIPPED_DT,
        ORDER_SHIPPED_FLAG AS ORDER_SHIPPED_FLAG,
        INTERNAL_NOTES AS INTERNAL_NOTES,
        PUBLIC_NOTES AS PUBLIC_NOTES,
        AUTOSHIP_DISCOUNT_AMT AS AUTOSHIP_DISCOUNT_AMT,
        ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
        RISKORDER_FLAG AS RISKORDER_FLAG,
        RISK_REASON AS RISK_REASON,
        ORIG_SHIP_METHOD_CD AS ORIG_SHIP_METHOD_CD,
        SHIP_HOLD_FLAG AS SHIP_HOLD_FLAG,
        SHIP_HOLD_DT AS SHIP_HOLD_DT,
        SHIP_RELEASE_DT AS SHIP_RELEASE_DT,
        ORDER_QTY AS ORDER_QTY,
        ITEM_DESC AS ITEM_DESC,
        EXT_PRICE AS EXT_PRICE,
        ORDER_DETAIL_CREATED_DT AS ORDER_DETAIL_CREATED_DT,
        ORDER_DETAIL_MODIFIED_DT AS ORDER_DETAIL_MODIFIED_DT,
        HOW_TO_GET_RX AS HOW_TO_GET_RX,
        VET_CD AS VET_CD,
        PET_CD AS PET_CD,
        ORDER_DETAIL_ONHOLD_FLAG AS ORDER_DETAIL_ONHOLD_FLAG,
        ONHOLD_TO_FILL_FLAG AS ONHOLD_TO_FILL_FLAG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_ORDER_DAY""")

df_0.createOrReplaceTempView("ALLIVET_ORDER_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_ORDER_DAY_1


df_1=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR,
        ORDER_STATUS,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            ALLIVET_ORDER_NBR,
            ORDER_STATUS,
            ROW_NUMBER() OVER (PARTITION 
        BY
            ALLIVET_ORDER_NBR 
        ORDER BY
            UPDATE_TSTMP DESC) RN 
        FROM
            ALLIVET_ORDER_DAY 
        WHERE
            ALLIVET_ORDER_NBR IN (
                SELECT
                    ALLIVET_ORDER_NBR 
                FROM
                    (SELECT
                        DISTINCT ALLIVET_ORDER_NBR,
                        ORDER_STATUS 
                    FROM
                        ALLIVET_ORDER_DAY) A 
                GROUP BY
                    ALLIVET_ORDER_NBR 
                HAVING
                    COUNT(*) > 1
                )
        ) B 
    WHERE
        RN = 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_ORDER_DAY_1")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_TSTMP_2


df_2=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        ORDER_STATUS AS ORDER_STATUS,
        current_timestamp AS UPDATE_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ALLIVET_ORDER_DAY_1""")

df_2.createOrReplaceTempView("EXP_UPDATE_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, ALLIVET_ORDER_DAY


spark.sql("""INSERT INTO ALLIVET_ORDER_DAY SELECT ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
null AS ALLIVET_ORDER_LN_NBR,
null AS ALLIVET_ORDER_DT,
null AS PETSMART_ORDER_DT,
ORDER_STATUS AS ORDER_STATUS,
null AS PRODUCT_ID,
null AS PETSMART_ORDER_NBR,
null AS PETSMART_SKU_NBR,
null AS ALLIVET_SKU_NBR,
null AS SUB_TOTAL_AMT,
null AS FREIGHT_COST,
null AS TOTAL_AMT,
null AS SHIP_METHOD_CD,
null AS ORDER_VOIDED_FLAG,
null AS ORDER_ONHOLD_FLAG,
null AS ORDER_CREATED_DT,
null AS ORDER_MODIFIED_DT,
null AS SHIPPED_DT,
null AS ORDER_SHIPPED_FLAG,
null AS INTERNAL_NOTES,
null AS PUBLIC_NOTES,
null AS AUTOSHIP_DISCOUNT_AMT,
null AS ORDER_MERCHANT_NOTES,
null AS RISKORDER_FLAG,
null AS RISK_REASON,
null AS ORIG_SHIP_METHOD_CD,
null AS SHIP_HOLD_FLAG,
null AS SHIP_HOLD_DT,
null AS SHIP_RELEASE_DT,
null AS ORDER_QTY,
null AS ITEM_DESC,
null AS EXT_PRICE,
null AS ORDER_DETAIL_CREATED_DT,
null AS ORDER_DETAIL_MODIFIED_DT,
null AS HOW_TO_GET_RX,
null AS VET_CD,
null AS PET_CD,
null AS ORDER_DETAIL_ONHOLD_FLAG,
null AS ONHOLD_TO_FILL_FLAG,
UPDATE_TSTMP AS UPDATE_TSTMP,
null AS LOAD_TSTMP FROM EXP_UPDATE_TSTMP_2""")