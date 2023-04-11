# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MERCHCAT_ORG_0


df_0=spark.sql("""
    SELECT
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MERCH_GL_CATEGORY_CD AS MERCH_GL_CATEGORY_CD,
        MERCH_GL_CATEGORY_DESC AS MERCH_GL_CATEGORY_DESC,
        CATEGORY_ANALYST_ID AS CATEGORY_ANALYST_ID,
        CATEGORY_ANALYST_NM AS CATEGORY_ANALYST_NM,
        CATEGORY_REPLENISHMENT_MGR_ID AS CATEGORY_REPLENISHMENT_MGR_ID,
        CATEGORY_REPLENISHMENT_MGR_NM AS CATEGORY_REPLENISHMENT_MGR_NM,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MERCHCAT_ORG""")

df_0.createOrReplaceTempView("MERCHCAT_ORG_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MERCHCAT_ORG_1


df_1=spark.sql("""
    SELECT
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MERCH_GL_CATEGORY_CD AS MERCH_GL_CATEGORY_CD,
        MERCH_GL_CATEGORY_DESC AS MERCH_GL_CATEGORY_DESC,
        CATEGORY_ANALYST_ID AS CATEGORY_ANALYST_ID,
        CATEGORY_ANALYST_NM AS CATEGORY_ANALYST_NM,
        REPLENISMENT_MGR_ID AS REPLENISMENT_MGR_ID,
        REPLENISMENT_MGR_NM AS REPLENISMENT_MGR_NM,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MERCHCAT_ORG_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_MERCHCAT_ORG_1")

# COMMAND ----------
# DBTITLE 1, MerchCategory_2


df_2=spark.sql("""
    SELECT
        MerchCategoryCd AS MerchCategoryCd,
        MerchCategoryDesc AS MerchCategoryDesc,
        MerchClassCd AS MerchClassCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        GLCategoryCd AS GLCategoryCd,
        CategoryAnalystId AS CategoryAnalystId,
        PlanningInd AS PlanningInd,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchCategory""")

df_2.createOrReplaceTempView("MerchCategory_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MerchCategory_3


df_3=spark.sql("""
    SELECT
        MerchCategoryCd AS MerchCategoryCd,
        MerchCategoryDesc AS MerchCategoryDesc,
        MerchClassCd AS MerchClassCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        GLCategoryCd AS GLCategoryCd,
        CategoryAnalystId AS CategoryAnalystId,
        PlanningInd AS PlanningInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MerchCategory_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_MerchCategory_3")

# COMMAND ----------
# DBTITLE 1, LKP_MerchClass_4


df_4=spark.sql("""
    SELECT
        MerchClassCd AS MerchClassCd,
        MerchClassDesc AS MerchClassDesc,
        MerchDeptCd AS MerchDeptCd,
        UpdateTstmp AS UpdateTstmp,
        SQ_Shortcut_to_MerchCategory_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MerchClass 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_MerchCategory_3 
            ON MerchClassCd = SQ_Shortcut_to_MerchCategory_3.MerchClassCd""")

df_4.createOrReplaceTempView("LKP_MerchClass_4")

# COMMAND ----------
# DBTITLE 1, LKP_MerchDept_5


df_5=spark.sql("""
    SELECT
        MerchDeptCd AS MerchDeptCd,
        MerchDeptDesc AS MerchDeptDesc,
        UpdateTstmp AS UpdateTstmp,
        LKP_MerchClass_4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MerchDept 
    RIGHT OUTER JOIN
        LKP_MerchClass_4 
            ON MerchDeptCd = LKP_MerchClass_4.MerchDeptCd""")

df_5.createOrReplaceTempView("LKP_MerchDept_5")

# COMMAND ----------
# DBTITLE 1, LKP_GLCategory_6


df_6=spark.sql("""
    SELECT
        GLCategoryCd AS GLCategoryCd,
        GLCategoryDesc AS GLCategoryDesc,
        UpdateTstmp AS UpdateTstmp,
        SQ_Shortcut_to_MerchCategory_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        GLCategory 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_MerchCategory_3 
            ON GLCategoryCd = SQ_Shortcut_to_MerchCategory_3.GLCategoryCd""")

df_6.createOrReplaceTempView("LKP_GLCategory_6")

# COMMAND ----------
# DBTITLE 1, LKP_CategoryAnalyst_7


df_7=spark.sql("""
    SELECT
        CategoryAnalystId AS CategoryAnalystId,
        CategoryAnalystName AS CategoryAnalystName,
        ReplenishmentManagerId AS ReplenishmentManagerId,
        UpdateTstmp AS UpdateTstmp,
        SQ_Shortcut_to_MerchCategory_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        CategoryAnalyst 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_MerchCategory_3 
            ON CategoryAnalystId = SQ_Shortcut_to_MerchCategory_3.CategoryAnalystId""")

df_7.createOrReplaceTempView("LKP_CategoryAnalyst_7")

# COMMAND ----------
# DBTITLE 1, LKP_ReplenishmentManager_8


df_8=spark.sql("""
    SELECT
        ReplenishmentManagerId AS ReplenishmentManagerId,
        ReplenishmentManagerName AS ReplenishmentManagerName,
        UpdateTstmp AS UpdateTstmp,
        LKP_CategoryAnalyst_7.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ReplenishmentManager 
    RIGHT OUTER JOIN
        LKP_CategoryAnalyst_7 
            ON ReplenishmentManagerId = LKP_CategoryAnalyst_7.ReplenishmentManagerId""")

df_8.createOrReplaceTempView("LKP_ReplenishmentManager_8")

# COMMAND ----------
# DBTITLE 1, EXP_ConvertIDs_9


df_9=spark.sql("""
    SELECT
        SQ_Shortcut_to_MerchCategory_3.MerchCategoryCd AS MerchCategoryCd,
        IFF(IS_NUMBER(SQ_Shortcut_to_MerchCategory_3.MerchCategoryCd),
        (CAST(SQ_Shortcut_to_MerchCategory_3.MerchCategoryCd AS DECIMAL (38,
        0))),
        -1) AS SAP_Category_ID,
        IFF(IS_NUMBER(SQ_Shortcut_to_MerchCategory_3.MerchClassCd),
        (CAST(SQ_Shortcut_to_MerchCategory_3.MerchClassCd AS DECIMAL (38,
        0))),
        -1) AS SAP_Class_ID,
        IFF(IS_NUMBER(LKP_MerchDept_5.MerchDeptCd),
        (CAST(LKP_MerchDept_5.MerchDeptCd AS DECIMAL (38,
        0))),
        -1) AS SAP_Dept_ID,
        LKP_GLCategory_6.GLCategoryCd AS GLCategoryCd,
        LKP_GLCategory_6.GLCategoryDesc AS GLCategoryDesc,
        LKP_CategoryAnalyst_7.CategoryAnalystId AS CategoryAnalystId,
        LKP_CategoryAnalyst_7.CategoryAnalystName AS CategoryAnalystName,
        IFF(IS_NUMBER(LKP_ReplenishmentManager_8.ReplenishmentManagerId),
        (CAST(LKP_ReplenishmentManager_8.ReplenishmentManagerId AS DECIMAL (38,
        0))),
        NULL) AS Replenishment_MGR_ID,
        LKP_ReplenishmentManager_8.ReplenishmentManagerName AS ReplenishmentManagerName,
        LKP_ReplenishmentManager_8.ReplenishmentManagerId AS ReplenishmentManagerId,
        SQ_Shortcut_to_MerchCategory_3.MerchCategoryDesc AS MerchCategoryDesc,
        SQ_Shortcut_to_MerchCategory_3.MerchClassCd AS MerchClassCd,
        LKP_MerchClass_4.MerchClassDesc AS MerchClassDesc,
        LKP_MerchDept_5.MerchDeptCd AS MerchDeptCd,
        LKP_MerchDept_5.MerchDeptDesc AS MerchDeptDesc,
        LKP_CategoryAnalyst_7.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_CategoryAnalyst_7 
    INNER JOIN
        LKP_ReplenishmentManager_8 
            ON LKP_CategoryAnalyst_7.Monotonically_Increasing_Id = LKP_ReplenishmentManager_8.Monotonically_Increasing_Id 
    INNER JOIN
        LKP_GLCategory_6 
            ON LKP_CategoryAnalyst_7.Monotonically_Increasing_Id = LKP_GLCategory_6.Monotonically_Increasing_Id 
    INNER JOIN
        LKP_MerchClass_4 
            ON LKP_CategoryAnalyst_7.Monotonically_Increasing_Id = LKP_MerchClass_4.Monotonically_Increasing_Id 
    INNER JOIN
        LKP_MerchDept_5 
            ON LKP_CategoryAnalyst_7.Monotonically_Increasing_Id = LKP_MerchDept_5.Monotonically_Increasing_Id 
    INNER JOIN
        SQ_Shortcut_to_MerchCategory_3 
            ON LKP_CategoryAnalyst_7.Monotonically_Increasing_Id = SQ_Shortcut_to_MerchCategory_3.Monotonically_Increasing_Id""")

df_9.createOrReplaceTempView("EXP_ConvertIDs_9")

# COMMAND ----------
# DBTITLE 1, JNR_GetUpdates_10


df_10=spark.sql("""
    SELECT
        MASTER.SAP_Category_ID AS SAP_Category_ID,
        MASTER.SAP_Class_ID AS SAP_Class_ID,
        MASTER.SAP_Dept_ID AS SAP_Dept_ID,
        MASTER.GLCategoryCd AS GLCategoryCd,
        MASTER.GLCategoryDesc AS GLCategoryDesc,
        MASTER.CategoryAnalystId AS CategoryAnalystId,
        MASTER.CategoryAnalystName AS CategoryAnalystName,
        MASTER.Replenishment_MGR_ID AS Replenishment_MGR_ID,
        MASTER.ReplenishmentManagerName AS ReplenishmentManagerName,
        DETAIL.SAP_CATEGORY_ID AS SAP_CATEGORY_ID_DST,
        DETAIL.SAP_CLASS_ID AS SAP_CLASS_ID_DST,
        DETAIL.SAP_DEPT_ID AS SAP_DEPT_ID_DST,
        DETAIL.MERCH_GL_CATEGORY_CD AS MERCH_GL_CATEGORY_CD_DST,
        DETAIL.MERCH_GL_CATEGORY_DESC AS MERCH_GL_CATEGORY_DESC_DST,
        DETAIL.CATEGORY_ANALYST_ID AS CATEGORY_ANALYST_ID_DST,
        DETAIL.CATEGORY_ANALYST_NM AS CATEGORY_ANALYST_NM_DST,
        DETAIL.REPLENISMENT_MGR_ID AS REPLENISMENT_MGR_ID_DST,
        DETAIL.REPLENISMENT_MGR_NM AS REPLENISMENT_MGR_NM_DST,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_ConvertIDs_9 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_MERCHCAT_ORG_1 DETAIL 
            ON MASTER.SAP_Category_ID = DETAIL.SAP_CATEGORY_ID""")

df_10.createOrReplaceTempView("JNR_GetUpdates_10")

# COMMAND ----------
# DBTITLE 1, EXP_CheckUpdates_11


df_11=spark.sql("""
    SELECT
        SAP_Category_ID AS SAP_Category_ID,
        SAP_Class_ID AS SAP_Class_ID,
        SAP_Dept_ID AS SAP_Dept_ID,
        GLCategoryCd AS GLCategoryCd,
        GLCategoryDesc AS GLCategoryDesc,
        CategoryAnalystId AS CategoryAnalystId,
        CategoryAnalystName AS CategoryAnalystName,
        Replenishment_MGR_ID AS Replenishment_MGR_ID,
        ReplenishmentManagerName AS ReplenishmentManagerName,
        IFF(ISNULL(SAP_CATEGORY_ID_DST),
        DD_INSERT,
        IFF(SAP_Category_ID <> SAP_CATEGORY_ID_DST 
        OR SAP_Class_ID <> SAP_CLASS_ID_DST 
        OR SAP_Dept_ID <> SAP_DEPT_ID_DST 
        OR GLCategoryCd <> MERCH_GL_CATEGORY_CD_DST 
        OR GLCategoryDesc <> MERCH_GL_CATEGORY_DESC_DST 
        OR IFF(ISNULL(CategoryAnalystId),
        -1,
        CategoryAnalystId) <> IFF(ISNULL(CATEGORY_ANALYST_ID_DST),
        -1,
        CATEGORY_ANALYST_ID_DST) 
        OR IFF(ISNULL(CategoryAnalystName),
        'NA',
        CategoryAnalystName) <> IFF(ISNULL(CATEGORY_ANALYST_NM_DST),
        'NA',
        CATEGORY_ANALYST_NM_DST) 
        OR IFF(ISNULL(Replenishment_MGR_ID),
        -1,
        Replenishment_MGR_ID) <> IFF(ISNULL(REPLENISMENT_MGR_ID_DST),
        -1,
        REPLENISMENT_MGR_ID_DST) 
        OR IFF(ISNULL(ReplenishmentManagerName),
        'NA',
        ReplenishmentManagerName) <> IFF(ISNULL(REPLENISMENT_MGR_NM_DST),
        'NA',
        REPLENISMENT_MGR_NM_DST),
        DD_UPDATE,
        DD_REJECT)) AS UpdateStrategy,
        current_timestamp AS UpdateDt,
        LOAD_TSTMP AS LOAD_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP_NN,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_GetUpdates_10""")

df_11.createOrReplaceTempView("EXP_CheckUpdates_11")

# COMMAND ----------
# DBTITLE 1, FIL_RemoveRejects_12


df_12=spark.sql("""
    SELECT
        SAP_Category_ID AS SAP_Category_ID,
        SAP_Class_ID AS SAP_Class_ID,
        SAP_Dept_ID AS SAP_Dept_ID,
        GLCategoryCd AS GLCategoryCd,
        GLCategoryDesc AS GLCategoryDesc,
        CategoryAnalystId AS CategoryAnalystId,
        CategoryAnalystName AS CategoryAnalystName,
        Replenishment_MGR_ID AS Replenishment_MGR_ID,
        ReplenishmentManagerName AS ReplenishmentManagerName,
        UpdateStrategy AS UpdateStrategy,
        UpdateDt AS UpdateDt,
        LOAD_TSTMP_NN AS LOAD_TSTMP_NN,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_CheckUpdates_11 
    WHERE
        UpdateStrategy <> DD_REJECT""")

df_12.createOrReplaceTempView("FIL_RemoveRejects_12")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_13


df_13=spark.sql("""
    SELECT
        SAP_Category_ID AS SAP_Category_ID,
        SAP_Class_ID AS SAP_Class_ID,
        SAP_Dept_ID AS SAP_Dept_ID,
        GLCategoryCd AS GLCategoryCd,
        GLCategoryDesc AS GLCategoryDesc,
        CategoryAnalystId AS CategoryAnalystId,
        CategoryAnalystName AS CategoryAnalystName,
        Replenishment_MGR_ID AS Replenishment_MGR_ID,
        ReplenishmentManagerName AS ReplenishmentManagerName,
        UpdateStrategy AS UpdateStrategy,
        UpdateDt AS UpdateDt,
        LOAD_TSTMP_NN AS LOAD_TSTMP_NN,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_RemoveRejects_12""")

df_13.createOrReplaceTempView("UPDTRANS_13")

# COMMAND ----------
# DBTITLE 1, MERCHCAT_ORG


spark.sql("""INSERT INTO MERCHCAT_ORG SELECT SAP_Category_ID AS SAP_CATEGORY_ID,
SAP_Class_ID AS SAP_CLASS_ID,
SAP_Dept_ID AS SAP_DEPT_ID,
GLCategoryCd AS MERCH_GL_CATEGORY_CD,
GLCategoryDesc AS MERCH_GL_CATEGORY_DESC,
CategoryAnalystId AS CATEGORY_ANALYST_ID,
CategoryAnalystName AS CATEGORY_ANALYST_NM,
Replenishment_MGR_ID AS CATEGORY_REPLENISHMENT_MGR_ID,
ReplenishmentManagerName AS CATEGORY_REPLENISHMENT_MGR_NM,
UpdateDt AS UPDATE_TSTMP,
LOAD_TSTMP_NN AS LOAD_TSTMP FROM UPDTRANS_13""")