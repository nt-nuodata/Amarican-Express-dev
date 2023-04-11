# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Transformation not handled

# COMMAND ----------
# DBTITLE 1, Vendor_0


df_0=spark.sql("""
    SELECT
        VendorNbr AS VendorNbr,
        VendorAccountGroup AS VendorAccountGroup,
        VendorName AS VendorName,
        CountryCD AS CountryCD,
        StreetAddress AS StreetAddress,
        AddressLine1 AS AddressLine1,
        AddressLine2 AS AddressLine2,
        City AS City,
        State AS State,
        Zip AS Zip,
        PhoneNbr1 AS PhoneNbr1,
        PhoneNbr2 AS PhoneNbr2,
        FaxNbr AS FaxNbr,
        POBoxNbr AS POBoxNbr,
        POBoxPostalCd AS POBoxPostalCd,
        LanguageCd AS LanguageCd,
        IndustryCd AS IndustryCd,
        TimeZone AS TimeZone,
        Region AS Region,
        CentralDeletionFlag AS CentralDeletionFlag,
        CentralPostingBlock AS CentralPostingBlock,
        CentralPurchasingBlock AS CentralPurchasingBlock,
        TaxNbr1 AS TaxNbr1,
        TaxNbr2 AS TaxNbr2,
        CustomerReferenceNbr AS CustomerReferenceNbr,
        CompanyIDforTradingPartner AS CompanyIDforTradingPartner,
        PayByEDIFlag AS PayByEDIFlag,
        ReturnToVendorEligibilityFlag AS ReturnToVendorEligibilityFlag,
        AlternatePayeeVendorCd AS AlternatePayeeVendorCd,
        VIPCD AS VIPCD,
        InactiveFlag AS InactiveFlag,
        IsVendorSubRangeRelevant AS IsVendorSubRangeRelevant,
        IsSiteLevelRelevant AS IsSiteLevelRelevant,
        IsOneTimeVendor AS IsOneTimeVendor,
        IsAlternatePayeeAllowed AS IsAlternatePayeeAllowed,
        StandardCarrierAccessCd AS StandardCarrierAccessCd,
        BlueGreenFlag AS BlueGreenFlag,
        IDOAFlag AS IDOAFlag,
        CarrierConfirmationExpectedFlag AS CarrierConfirmationExpectedFlag,
        TransportationChainCd AS TransportationChainCd,
        TransitTimeDays AS TransitTimeDays,
        TMSTransportationModeID AS TMSTransportationModeID,
        SCACCd AS SCACCd,
        PerformanceRatingID AS PerformanceRatingID,
        TenderMethodID AS TenderMethodID,
        CarrierContactName AS CarrierContactName,
        CarrierContactEmailAddress AS CarrierContactEmailAddress,
        CarrierGroupName AS CarrierGroupName,
        ShipmentStatusRequiredFlag AS ShipmentStatusRequiredFlag,
        ApplyFixedCostAllFlag AS ApplyFixedCostAllFlag,
        PickupInServiceFlag AS PickupInServiceFlag,
        PickupCutoffFlag AS PickupCutoffFlag,
        ResponseTime AS ResponseTime,
        ABSMinimumCharge AS ABSMinimumCharge,
        CMMaxHours AS CMMaxHours,
        CMFreeMilesi AS CMFreeMilesi,
        CMCapableFlag AS CMCapableFlag,
        CMDiscount AS CMDiscount,
        CMDiscountFirstLegFlag AS CMDiscountFirstLegFlag,
        CMVariableRate AS CMVariableRate,
        PackageDiscount AS PackageDiscount,
        Notes AS Notes,
        VendorAddDT AS VendorAddDT,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Vendor""")

df_0.createOrReplaceTempView("Vendor_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_VENDOR1_1


df_1=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        DATE_VEND_ADDED AS DATE_VEND_ADDED,
        DATE_VEND_REFRESHED AS DATE_VEND_REFRESHED,
        EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
        VENDOR_CTRY_ABBR AS VENDOR_CTRY_ABBR,
        VENDOR_CTRY AS VENDOR_CTRY,
        VENDOR_NAME AS VENDOR_NAME,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
        INCOTERM AS INCOTERM,
        CASH_TERM AS CASH_TERM,
        PAYMENT_TERM AS PAYMENT_TERM,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Vendor_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_VENDOR1_1")

# COMMAND ----------
# DBTITLE 1, VENDOR


spark.sql("""INSERT INTO VENDOR SELECT VENDOR_ID AS VENDOR_ID,
DATE_VEND_ADDED AS DATE_VEND_ADDED,
DATE_VEND_REFRESHED AS DATE_VEND_REFRESHED,
EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
VENDOR_CTRY_ABBR AS VENDOR_CTRY_ABBR,
VENDOR_CTRY AS VENDOR_CTRY,
VENDOR_NAME AS VENDOR_NAME,
PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
INCOTERM AS INCOTERM,
CASH_TERM AS CASH_TERM,
PAYMENT_TERM AS PAYMENT_TERM FROM SQ_Shortcut_to_VENDOR1_1""")