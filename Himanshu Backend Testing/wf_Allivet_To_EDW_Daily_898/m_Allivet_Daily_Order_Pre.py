# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Shortcut_to_Allivet_Daily_Order


df_0 = spark.sql("""SELECT
  AllivetOrderCode AS AllivetOrderCode,
  PetsmartOrderCode AS PetsmartOrderCode,
  AllivetOrderDate AS AllivetOrderDate,
  PetsmartOrderDate AS PetsmartOrderDate,
  Subtotal AS Subtotal,
  Freight AS Freight,
  Total AS Total,
  ShippingMethodCode AS ShippingMethodCode,
  OrderStatus AS OrderStatus,
  IsVoided AS IsVoided,
  IsOnHold AS IsOnHold,
  SalesOrderDateCreated AS SalesOrderDateCreated,
  SalesOrderDateModified AS SalesOrderDateModified,
  DateShipped AS DateShipped,
  IsShipped AS IsShipped,
  InternalNotes AS InternalNotes,
  PublicNotes AS PublicNotes,
  AutoShip_DiscountAmount AS AutoShip_DiscountAmount,
  OrderMerchantNotes AS OrderMerchantNotes,
  IsRiskOrder AS IsRiskOrder,
  RiskReasons AS RiskReasons,
  OriginalShippingMethodCode AS OriginalShippingMethodCode,
  IsShipHold AS IsShipHold,
  DateShipHold AS DateShipHold,
  DateShipReleased AS DateShipReleased,
  AllivetSku AS AllivetSku,
  PetsmartSku AS PetsmartSku,
  LineNum AS LineNum,
  QuantityOrdered AS QuantityOrdered,
  ItemDescription AS ItemDescription,
  ExtPrice AS ExtPrice,
  CustomerSalesOrderDetailDateCreated AS CustomerSalesOrderDetailDateCreated,
  CustomerSalesOrderDetailDateModified AS CustomerSalesOrderDetailDateModified,
  HowdowegetyoutRx AS HowdowegetyoutRx,
  VetCode AS VetCode,
  PetCode AS PetCode,
  IsOnHold1 AS IsOnHold1,
  IsOnHoldToFill_C AS IsOnHoldToFill_C
FROM
  Allivet_Daily_Order""")

df_0.createOrReplaceTempView("Shortcut_to_Allivet_Daily_Order_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Allivet_Daily_Order


df_1 = spark.sql("""SELECT
  AllivetOrderCode AS AllivetOrderCode,
  PetsmartOrderCode AS PetsmartOrderCode,
  AllivetOrderDate AS AllivetOrderDate,
  PetsmartOrderDate AS PetsmartOrderDate,
  Subtotal AS Subtotal,
  Freight AS Freight,
  Total AS Total,
  ShippingMethodCode AS ShippingMethodCode,
  OrderStatus AS OrderStatus,
  IsVoided AS IsVoided,
  IsOnHold AS IsOnHold,
  SalesOrderDateCreated AS SalesOrderDateCreated,
  SalesOrderDateModified AS SalesOrderDateModified,
  DateShipped AS DateShipped,
  IsShipped AS IsShipped,
  InternalNotes AS InternalNotes,
  PublicNotes AS PublicNotes,
  AutoShip_DiscountAmount AS AutoShip_DiscountAmount,
  OrderMerchantNotes AS OrderMerchantNotes,
  IsRiskOrder AS IsRiskOrder,
  RiskReasons AS RiskReasons,
  OriginalShippingMethodCode AS OriginalShippingMethodCode,
  IsShipHold AS IsShipHold,
  DateShipHold AS DateShipHold,
  DateShipReleased AS DateShipReleased,
  AllivetSku AS AllivetSku,
  PetsmartSku AS PetsmartSku,
  LineNum AS LineNum,
  QuantityOrdered AS QuantityOrdered,
  ItemDescription AS ItemDescription,
  ExtPrice AS ExtPrice,
  CustomerSalesOrderDetailDateCreated AS CustomerSalesOrderDetailDateCreated,
  CustomerSalesOrderDetailDateModified AS CustomerSalesOrderDetailDateModified,
  HowdowegetyoutRx AS HowdowegetyoutRx,
  VetCode AS VetCode,
  PetCode AS PetCode,
  IsOnHold1 AS IsOnHold1,
  IsOnHoldToFill_C AS IsOnHoldToFill_C,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Allivet_Daily_Order_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Allivet_Daily_Order_1")

# COMMAND ----------
# DBTITLE 1, exp_load_tstmp


df_2 = spark.sql("""SELECT
  AllivetOrderCode AS AllivetOrderCode,
  PetsmartOrderCode AS PetsmartOrderCode,
  TO_DATE(AllivetOrderDate, 'MM-DD-YYYY HH12:MI:SSAM') AS o_AllivetOrderDate,
  TO_DATE(PetsmartOrderDate, 'MM-DD-YY HH24:MI') AS o_PetsmartOrderDate,
  Subtotal AS Subtotal,
  Freight AS Freight,
  Total AS Total,
  ShippingMethodCode AS ShippingMethodCode,
  OrderStatus AS OrderStatus,
  IFF(
    Upper(IsVoided) = 'FALSE',
    '0',
    IFF(Upper(IsVoided) = 'TRUE', '1', IsVoided)
  ) AS o_IsVoided,
  IFF(
    Upper(IsOnHold) = 'FALSE',
    '0',
    IFF(Upper(IsOnHold) = 'TRUE', '1', IsOnHold)
  ) AS o_IsOnHold,
  TO_DATE(SalesOrderDateCreated, 'MM-DD-YYYY HH12:MI:SSAM') AS o_SalesOrderDateCreated,
  TO_DATE(SalesOrderDateModified, 'MM-DD-YYYY HH12:MI:SSAM') AS o_SalesOrderDateModified,
  TO_DATE(DateShipped, 'MM-DD-YYYY HH12:MI:SSAM') AS o_DateShipped,
  IFF(
    Upper(IsShipped) = 'FALSE',
    '0',
    IFF(Upper(IsShipped) = 'TRUE', '1', IsShipped)
  ) AS o_IsShipped,
  InternalNotes AS InternalNotes,
  PublicNotes AS PublicNotes,
  AutoShip_DiscountAmount AS AutoShip_DiscountAmount,
  OrderMerchantNotes AS OrderMerchantNotes,
  IFF(
    Upper(IsRiskOrder) = 'FALSE',
    '0',
    IFF(Upper(IsRiskOrder) = 'TRUE', '1', IsRiskOrder)
  ) AS o_IsRiskOrder,
  RiskReasons AS RiskReasons,
  OriginalShippingMethodCode AS OriginalShippingMethodCode,
  IFF(
    Upper(IsShipHold) = 'FALSE',
    '0',
    IFF(Upper(IsShipHold) = 'TRUE', '1', IsShipHold)
  ) AS o_IsShipHold,
  TO_DATE(DateShipHold, 'MM-DD-YYYY HH12:MI:SSAM') AS o_DateShipHold,
  TO_DATE(DateShipReleased, 'MM-DD-YYYY HH12:MI:SSAM') AS o_DateShipReleased,
  AllivetSku AS AllivetSku,
  PetsmartSku AS PetsmartSku,
  LineNum AS LineNum,
  QuantityOrdered AS QuantityOrdered,
  ItemDescription AS ItemDescription,
  ExtPrice AS ExtPrice,
  TO_DATE(
    CustomerSalesOrderDetailDateCreated,
    'MM-DD-YYYY HH12:MI:SSAM'
  ) AS o_CustomerSalesOrderDetailDateCreated,
  TO_DATE(
    CustomerSalesOrderDetailDateModified,
    'MM-DD-YYYY HH12:MI:SSAM'
  ) AS o_CustomerSalesOrderDetailDateModified,
  HowdowegetyoutRx AS HowdowegetyoutRx,
  VetCode AS VetCode,
  PetCode AS PetCode,
  IFF(
    Upper(IsOnHold1) = 'FALSE',
    '0',
    IFF(Upper(IsOnHold1) = 'TRUE', '1', IsOnHold1)
  ) AS IsShipHold,
  IFF(
    Upper(IsOnHoldToFill_C) = 'FALSE',
    '0',
    IFF(
      Upper(IsOnHoldToFill_C) = 'TRUE',
      '1',
      IsOnHoldToFill_C
    )
  ) AS IsOnHoldToFill_C1,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Allivet_Daily_Order_1""")

df_2.createOrReplaceTempView("exp_load_tstmp_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ALLIVET_DAILY_ORDER_PRE


spark.sql("""INSERT INTO
  ALLIVET_DAILY_ORDER_PRE_V2_test
SELECT
  AllivetOrderCode AS ALLIVET_ORDER_CODE,
  LineNum AS ALLIVET_ORDER_LINE_NUMBER,
  o_AllivetOrderDate AS ALLIVET_ORDER_DATE,
  o_PetsmartOrderDate AS PETSMART_ORDER_DATE,
  OrderStatus AS ORDER_STATUS,
  PetsmartOrderCode AS PETSMART_ORDER_CODE,
  Subtotal AS SUB_TOTAL,
  Freight AS FREIGHT,
  Total AS TOTAL,
  ShippingMethodCode AS SHIPPING_METHOD_CODE,
  o_IsVoided AS IS_ORDER_VOIDED,
  o_IsOnHold AS IS_ORDER_ONHOLD,
  o_SalesOrderDateCreated AS ORDER_CREATED_DATE,
  o_SalesOrderDateModified AS ORDER_MODIFIED_DATE,
  o_DateShipped AS SHIPPED_DATE,
  o_IsShipped AS IS_ORDER_SHIPPED,
  InternalNotes AS INTERNAL_NOTES,
  PublicNotes AS PUBLIC_NOTES,
  AutoShip_DiscountAmount AS AUTOSHIP_DISCOUNT_AMOUNT,
  OrderMerchantNotes AS ORDER_MERCHANT_NOTES,
  o_IsRiskOrder AS IS_RISKORDER,
  RiskReasons AS RISK_REASONS,
  OriginalShippingMethodCode AS ORIGINAL_SHIPPING_METHOD_CODE,
  o_IsShipHold AS IS_SHIPHOLD,
  o_DateShipHold AS SHIPHOLD_DATE,
  o_DateShipReleased AS SHIPRELEASED_DATE,
  AllivetSku AS ALLIVET_SKU,
  PetsmartSku AS PETSMART_SKU,
  QuantityOrdered AS ORDERED_QUANTITY,
  ItemDescription AS ITEM_DESCRIPTION,
  ExtPrice AS EXT_PRICE,
  o_CustomerSalesOrderDetailDateCreated AS ORDER_DETAIL_CREATED_DATE,
  o_CustomerSalesOrderDetailDateModified AS ORDER_DETAIL_MODIFIED_DATE,
  HowdowegetyoutRx AS HOW_TO_GET_RX,
  VetCode AS VET_CODE,
  PetCode AS PET_CODE,
  IsShipHold AS IS_ORDER_DETAIL_ONHOLD,
  IsOnHoldToFill_C1 AS IS_ONHOLD_TOFILL,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  exp_load_tstmp_2""")