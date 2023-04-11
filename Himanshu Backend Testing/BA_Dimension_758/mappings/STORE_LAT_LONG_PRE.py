# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Address_0


df_0=spark.sql("""
    SELECT
        AddressId AS AddressId,
        StreetLine1 AS StreetLine1,
        StreetLine2 AS StreetLine2,
        City AS City,
        StateProvinceId AS StateProvinceId,
        ZipCode AS ZipCode,
        Location AS Location,
        Latitude AS Latitude,
        Longitude AS Longitude,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Address""")

df_0.createOrReplaceTempView("Address_0")

# COMMAND ----------
# DBTITLE 1, Store_1


df_1=spark.sql("""
    SELECT
        StoreId AS StoreId,
        StoreNumber AS StoreNumber,
        AddressId AS AddressId,
        DistrictId AS DistrictId,
        Name AS Name,
        PhoneNumber AS PhoneNumber,
        IsActive AS IsActive,
        IsRelocation AS IsRelocation,
        OpeningDate AS OpeningDate,
        SoftLaunchDate AS SoftLaunchDate,
        InStorePickupEffectiveDate AS InStorePickupEffectiveDate,
        PhotoId AS PhotoId,
        NeedsReview AS NeedsReview,
        CreatedBy AS CreatedBy,
        CreatedDate AS CreatedDate,
        LastModifiedBy AS LastModifiedBy,
        LastModifiedDate AS LastModifiedDate,
        IsConcept AS IsConcept,
        GooglePlaceId AS GooglePlaceId,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Store""")

df_1.createOrReplaceTempView("Store_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Address_2


df_2=spark.sql("""
    SELECT
        a.latitude,
        a.longitude,
        s.storenumber 
    FROM
        store s 
    INNER JOIN
        address a 
            ON s.addressid = a.addressid 
    ORDER BY
        s.storenumber""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_Address_2")

# COMMAND ----------
# DBTITLE 1, STORE_LAT_LONG_PRE


spark.sql("""INSERT INTO STORE_LAT_LONG_PRE SELECT StoreNumber AS STORE_NBR,
Latitude AS LAT,
Longitude AS LON FROM SQ_Shortcut_to_Address_2""")