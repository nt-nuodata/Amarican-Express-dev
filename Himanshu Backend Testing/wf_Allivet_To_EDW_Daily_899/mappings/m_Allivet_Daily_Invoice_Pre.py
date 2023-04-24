# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Shortcut_to_Allivet_Daily_Invoice_0


df_0 = spark.sql("""SELECT
  InvoicePostingDate AS InvoicePostingDate,
  InvoiceCode AS InvoiceCode,
  TransactionType AS TransactionType,
  Allivet_Order AS Allivet_Order,
  PetSmart_Order AS PetSmart_Order,
  AllivetSku AS AllivetSku,
  PetSmartSku AS PetSmartSku,
  UPC AS UPC,
  UnitsSold AS UnitsSold,
  ProductCost AS ProductCost,
  RetailPrice AS RetailPrice,
  Manufacturer AS Manufacturer,
  Brand AS Brand,
  Title AS Title,
  FreightFee AS FreightFee,
  PackagingFee AS PackagingFee,
  DispensingFee AS DispensingFee,
  shipped_on AS shipped_on,
  fulfillment_origin_zip_code AS fulfillment_origin_zip_code,
  shipping_carrier AS shipping_carrier,
  TrackingNumber AS TrackingNumber,
  Customer_id AS Customer_id,
  Pet_name AS Pet_name,
  Pet_type AS Pet_type,
  Breed_type AS Breed_type,
  Pet_gender AS Pet_gender,
  Pet_is_pregnant AS Pet_is_pregnant,
  pet_DOB AS pet_DOB,
  Pet_weight AS Pet_weight,
  Pet_allergy AS Pet_allergy,
  Pet_medical_conditions AS Pet_medical_conditions,
  Vet_clinic_name AS Vet_clinic_name,
  Veterinary_name AS Veterinary_name,
  Vet_address AS Vet_address,
  Vet_city AS Vet_city,
  Vet_state AS Vet_state,
  Vet_zip AS Vet_zip,
  Vet_phone_number AS Vet_phone_number
FROM
  Allivet_Daily_Invoice""")

df_0.createOrReplaceTempView("Shortcut_to_Allivet_Daily_Invoice_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Allivet_Daily_Invoice_1


df_1 = spark.sql("""SELECT
  InvoicePostingDate AS InvoicePostingDate,
  InvoiceCode AS InvoiceCode,
  TransactionType AS TransactionType,
  Allivet_Order AS Allivet_Order,
  PetSmart_Order AS PetSmart_Order,
  AllivetSku AS AllivetSku,
  PetSmartSku AS PetSmartSku,
  UPC AS UPC,
  UnitsSold AS UnitsSold,
  ProductCost AS ProductCost,
  RetailPrice AS RetailPrice,
  Manufacturer AS Manufacturer,
  Brand AS Brand,
  Title AS Title,
  FreightFee AS FreightFee,
  PackagingFee AS PackagingFee,
  DispensingFee AS DispensingFee,
  shipped_on AS shipped_on,
  fulfillment_origin_zip_code AS fulfillment_origin_zip_code,
  shipping_carrier AS shipping_carrier,
  TrackingNumber AS TrackingNumber,
  Customer_id AS Customer_id,
  Pet_name AS Pet_name,
  Pet_type AS Pet_type,
  Breed_type AS Breed_type,
  Pet_gender AS Pet_gender,
  pet_DOB AS pet_DOB,
  Pet_weight AS Pet_weight,
  Pet_allergy AS Pet_allergy,
  Pet_medical_conditions AS Pet_medical_conditions,
  Pet_is_pregnant AS Pet_is_pregnant,
  Vet_clinic_name AS Vet_clinic_name,
  Veterinary_name AS Veterinary_name,
  Vet_address AS Vet_address,
  Vet_city AS Vet_city,
  Vet_state AS Vet_state,
  Vet_zip AS Vet_zip,
  Vet_phone_number AS Vet_phone_number,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Allivet_Daily_Invoice_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Allivet_Daily_Invoice_1")

# COMMAND ----------
# DBTITLE 1, Exp_LoadTstmp_2


df_2 = spark.sql("""SELECT
  InvoicePostingDate AS i_InvoicePostingDate,
  To_date(InvoicePostingDate, 'YYYY-MM-DD') AS o_InvoicePostingDate,
  InvoiceCode AS InvoiceCode,
  TransactionType AS TransactionType,
  Allivet_Order AS Allivet_Order,
  PetSmart_Order AS PetSmart_Order,
  AllivetSku AS AllivetSku,
  PetSmartSku AS PetSmartSku,
  UPC AS UPC,
  UnitsSold AS UnitsSold,
  ProductCost AS ProductCost,
  RetailPrice AS RetailPrice,
  Manufacturer AS Manufacturer,
  Brand AS Brand,
  Title AS Title,
  FreightFee AS FreightFee,
  PackagingFee AS PackagingFee,
  DispensingFee AS DispensingFee,
  to_date(shipped_on, 'YYYY-MM-DD HH24:MI:SS.MS') AS o_shipped_on,
  fulfillment_origin_zip_code AS fulfillment_origin_zip_code,
  shipping_carrier AS shipping_carrier,
  TrackingNumber AS TrackingNumber,
  Customer_id AS Customer_id,
  Pet_name AS Pet_name,
  Pet_type AS Pet_type,
  Breed_type AS Breed_type,
  Pet_gender AS Pet_gender,
  TO_DATE(pet_DOB, 'YYYY-MM-DD') AS o_pet_DOB,
  Pet_weight AS Pet_weight,
  Pet_allergy AS Pet_allergy,
  Pet_medical_conditions AS Pet_medical_conditions,
  IFF (
    ISNULL(Pet_is_pregnant),
    NULL,
    IFF(
      UPPER(Pet_is_pregnant) = 'Y',
      1,
      IFF(UPPER(Pet_is_pregnant) = 'N', 0)
    )
  ) AS o_Pet_is_pregnant,
  Vet_clinic_name AS Vet_clinic_name,
  Veterinary_name AS Veterinary_name,
  Vet_address AS Vet_address,
  Vet_city AS Vet_city,
  Vet_state AS Vet_state,
  Vet_zip AS Vet_zip,
  Vet_phone_number AS Vet_phone_number,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Allivet_Daily_Invoice_1""")

df_2.createOrReplaceTempView("Exp_LoadTstmp_2")

# COMMAND ----------
# DBTITLE 1, ALLIVET_DAILY_INVOICE_PRE


spark.sql("""INSERT INTO
  ALLIVET_DAILY_INVOICE_PRE
SELECT
  o_InvoicePostingDate AS INVOICE_POSTING_DATE,
  InvoiceCode AS INVOICE_CODE,
  TransactionType AS TRANSACTION_TYPE,
  Allivet_Order AS ALLIVET_ORDER_CODE,
  AllivetSku AS ALLIVET_SKU,
  PetSmart_Order AS PETSMART_ORDER_CODE,
  PetSmartSku AS PETSMART_SKU,
  UPC AS UPC,
  UnitsSold AS UNITS_SOLD,
  ProductCost AS PRODUCT_COST,
  RetailPrice AS RETAIL_PRICE,
  Manufacturer AS MANUFACTURER,
  Brand AS BRAND,
  Title AS TITLE,
  FreightFee AS FREIGHT_FEE,
  PackagingFee AS PACKAGING_FEE,
  DispensingFee AS DISPENSING_FEE,
  o_shipped_on AS SHIPPED_DATE,
  fulfillment_origin_zip_code AS FULFILLMENT_ORIGIN_ZIP_CODE,
  shipping_carrier AS SHIPPING_CARRIER,
  TrackingNumber AS TRACKING_NUMBER,
  Customer_id AS ALLIVET_CUSTOMER_ID,
  Pet_name AS PET_NAME,
  Pet_type AS PET_TYPE,
  Breed_type AS BREED_TYPE,
  Pet_gender AS PET_GENDER,
  o_pet_DOB AS PET_DOB,
  Pet_weight AS PET_WEIGHT,
  Pet_allergy AS PET_ALLERGY,
  Pet_medical_conditions AS PET_MEDICAL_CONDITIONS,
  o_Pet_is_pregnant AS PET_IS_PREGNANT,
  Vet_clinic_name AS VET_CLINIC_NAME,
  Veterinary_name AS VETERINARY_NAME,
  Vet_address AS VET_ADDRESS,
  Vet_city AS VET_CITY,
  Vet_state AS VET_STATE,
  Vet_zip AS VET_ZIP,
  Vet_phone_number AS VET_PHONE_NUMBER,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  Exp_LoadTstmp_2""")