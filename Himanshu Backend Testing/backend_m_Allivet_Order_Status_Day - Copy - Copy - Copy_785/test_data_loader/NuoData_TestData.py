# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DELTA_TRAINING;

# COMMAND ----------

# MAGIC %md
# MAGIC The InfoTypes available
# MAGIC ======================
# MAGIC 'aba', 'add_provider', 'address', 'administrative_unit', 'am_pm', 'android_platform_token', 'ascii_company_email', 'ascii_email', 'ascii_free_email', 'ascii_safe_email', 'bank_country', 'bban', 'binary', 'boolean', 'bothify', 'bs', 'building_number', 'cache_pattern', 'catch_phrase', 'century', 'chrome', 'city', 'city_prefix', 'city_suffix', 'color', 'color_name', 'company', 'company_email', 'company_suffix', 'coordinate', 'country', 'country_calling_code', 'country_code', 'credit_card_expire', 'credit_card_full', 'credit_card_number', 'credit_card_provider', 'credit_card_security_code', 'cryptocurrency', 'cryptocurrency_code', 'cryptocurrency_name', 'csv', 'currency', 'currency_code', 'currency_name', 'currency_symbol', 'current_country', 'current_country_code', 'date', 'date_between', 'date_between_dates', 'date_object', 'date_of_birth', 'date_this_century', 'date_this_decade', 'date_this_month', 'date_this_year', 'date_time', 'date_time_ad', 'date_time_between', 'date_time_between_dates', 'date_time_this_century', 'date_time_this_decade', 'date_time_this_month', 'date_time_this_year', 'day_of_month', 'day_of_week', 'del_arguments', 'dga', 'domain_name', 'domain_word', 'dsv', 'ean', 'ean13', 'ean8', 'ein', 'email', 'emoji', 'enum', 'factories', 'file_extension', 'file_name', 'file_path', 'firefox', 'first_name', 'first_name_female', 'first_name_male', 'first_name_nonbinary', 'fixed_width', 'format', 'free_email', 'free_email_domain', 'future_date', 'future_datetime', 'generator_attrs', 'get_arguments', 'get_formatter', 'get_providers', 'hex_color', 'hexify', 'hostname', 'http_method', 'iana_id', 'iban', 'image', 'image_url', 'internet_explorer', 'invalid_ssn', 'ios_platform_token', 'ipv4', 'ipv4_network_class', 'ipv4_private', 'ipv4_public', 'ipv6', 'isbn10', 'isbn13', 'iso8601', 'items', 'itin', 'job', 'json', 'json_bytes', 'language_code', 'language_name', 'last_name', 'last_name_female', 'last_name_male', 'last_name_nonbinary', 'latitude', 'latlng', 'lexify', 'license_plate', 'linux_platform_token', 'linux_processor', 'local_latlng', 'locale', 'locales', 'localized_ean', 'localized_ean13', 'localized_ean8', 'location_on_land', 'longitude', 'mac_address', 'mac_platform_token', 'mac_processor', 'md5', 'military_apo', 'military_dpo', 'military_ship', 'military_state', 'mime_type', 'month', 'month_name', 'msisdn', 'name', 'name_female', 'name_male', 'name_nonbinary', 'nic_handle', 'nic_handles', 'null_boolean', 'numerify', 'opera', 'paragraph', 'paragraphs', 'parse', 'password', 'past_date', 'past_datetime', 'phone_number', 'port_number', 'postalcode', 'postalcode_in_state', 'postalcode_plus4', 'postcode', 'postcode_in_state', 'prefix', 'prefix_female', 'prefix_male', 'prefix_nonbinary', 'pricetag', 'profile', 'provider', 'providers', 'psv', 'pybool', 'pydecimal', 'pydict', 'pyfloat', 'pyint', 'pyiterable', 'pylist', 'pyset', 'pystr', 'pystr_format', 'pystruct', 'pytimezone', 'pytuple', 'random', 'random_choices', 'random_digit', 'random_digit_not_null', 'random_digit_not_null_or_empty', 'random_digit_or_empty', 'random_element', 'random_elements', 'random_int', 'random_letter', 'random_letters', 'random_lowercase_letter', 'random_number', 'random_sample', 'random_uppercase_letter', 'randomize_nb_elements', 'rgb_color', 'rgb_css_color', 'ripe_id', 'safari', 'safe_color_name', 'safe_domain_name', 'safe_email', 'safe_hex_color', 'secondary_address', 'seed', 'seed_instance', 'seed_locale', 'sentence', 'sentences', 'set_arguments', 'set_formatter', 'sha1', 'sha256', 'simple_profile', 'slug', 'ssn', 'state', 'state_abbr', 'street_address', 'street_name', 'street_suffix', 'suffix', 'suffix_female', 'suffix_male', 'suffix_nonbinary', 'swift', 'swift11', 'swift8', 'tar', 'text', 'texts', 'time', 'time_delta', 'time_object', 'time_series', 'timezone', 'tld', 'tsv', 'unique', 'unix_device', 'unix_partition', 'unix_time', 'upc_a', 'upc_e', 'uri', 'uri_extension', 'uri_page', 'uri_path', 'url', 'user_agent', 'user_name', 'uuid4', 'weights', 'windows_platform_token', 'word', 'words', 'year', 'zip', 'zipcode', 'zipcode_in_state', 'zipcode_plus4'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.ALLIVET_INVOICE_DAY(INVOICE_POSTING_DT DATE,
# MAGIC INVOICE_CD STRING,
# MAGIC TXN_TYPE STRING,
# MAGIC ALLIVET_ORDER_NBR STRING,
# MAGIC ALLIVET_SKU_NBR STRING,
# MAGIC PRODUCT_ID INT,
# MAGIC PETSMART_ORDER_NBR STRING,
# MAGIC PETSMART_SKU_NBR INT,
# MAGIC UPC_ID BIGINT,
# MAGIC SOLD_UNITS_QTY INT,
# MAGIC PRODUCT_COST INT,
# MAGIC RETAIL_PRICE INT,
# MAGIC MANUFACTURER STRING,
# MAGIC BRAND_NAME STRING,
# MAGIC TITLE STRING,
# MAGIC FREIGHT_FEE_AMT INT,
# MAGIC PACKAGING_FEE_AMT INT,
# MAGIC DISPENSING_FEE_AMT INT,
# MAGIC SHIPPED_DT DATE,
# MAGIC FULFILLMENT_ORIGIN_ZIP_CD STRING,
# MAGIC SHIP_CARRIER_NAME STRING,
# MAGIC TRACKING_NBR STRING,
# MAGIC ALLIVET_CUSTOMER_NBR STRING,
# MAGIC PET_NAME STRING,
# MAGIC PET_TYPE STRING,
# MAGIC BREED_TYPE STRING,
# MAGIC PET_GENDER STRING,
# MAGIC PET_BIRT_DT DATE,
# MAGIC PET_WEIGHT INT,
# MAGIC PET_ALLERGY_DESC STRING,
# MAGIC PET_MEDICAL_CONDITION STRING,
# MAGIC PET_PREGNANT_FLAG INT,
# MAGIC VET_CLINIC_NAME STRING,
# MAGIC VET_NAME STRING,
# MAGIC VET_ADDRESS STRING,
# MAGIC VET_CITY STRING,
# MAGIC VET_STATE_CD STRING,
# MAGIC VET_ZIP_CD STRING,
# MAGIC VET_PHONE_NBR STRING,
# MAGIC DELETE_FLAG INT,
# MAGIC UPDATE_TSTMP TIMESTAMP,
# MAGIC LOAD_TSTMP TIMESTAMP) USING DELTA;

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from faker import Faker

# COMMAND ----------

fake = Faker()
date=['DT','DATE','TSTMP']
number=['ID','NBR','CD','CNT','MO']
amt=['AMT','PRICE','COST','QTY']
dog_names=['Ember','Tokyo','Heaven','Tuffy','Barkey','Oscar']
TYPE=['Rabbit','Horse','Bird','Rat','Ferret','Fish']
breed=['Arab','Californian','Catfishes','Sparrow','Catfishes']
allergy=['Sneezing','Cough','Red-eyes']
med_cond=['Distemper','Heartworms','Allergies','SkinInfection']
txn=['Cash','Debit-Card','Paytm']
brand= ['Orijen','Nestlé','Mammoth']

# COMMAND ----------

import random
generate_random_data_udf = udf(lambda x:generate_random_data(x), StringType())
def generate_random_data(col_name):
    sub_col=col_name.split('_')
    for sub in sub_col:
        if sub in number and sub!='PHONE' :
             return fake.random_number()
        elif sub=='PHONE':
             return fake.phone_number()
    for sub in sub_col:
        if sub in date:
             return fake.date()
        if sub in amt:
                return fake.random_int(1,50)    
        if 'PET' in sub_col:
             if 'TYPE' in col_name:
                return random.choice((TYPE))
             elif 'GENDER' in col_name:
                return random.choice(['M','F'])
             elif 'NAME' in col_name:
                return random.choice(dog_names)
             elif 'WEIGHT' in col_name:
                return fake.random_int(10,50)
             elif 'ALLERGY' in col_name:
                return random.choice((allergy))
        if 'BREED' in sub_col:
             return random.choice(breed)
        if 'FLAG' in col_name:
                return fake.random.choice([0,1])
        if 'ADDRESS' in col_name:
                return fake.address()
        if 'CITY' in col_name:
                return fake.city()
        if 'MEDICAL' in col_name:
                return random.choice((med_cond))
        if 'TXN' in col_name:
                return random.choice((txn))
        if 'BRAND' in col_name:
                return random.choice((brand))       
    else:
        return str(fake.name())

# COMMAND ----------

df = spark.sql("""select * from DELTA_TRAINING.ALLIVET_INVOICE_DAY""")
df1 = spark.range(10000).withColumn('id', lit(0).cast(IntegerType()))
for col in df.dtypes:
    df1=df1.withColumn(col[0],generate_random_data_udf(lit(col[0])))
    df1=df1.drop("id")

# COMMAND ----------

df1.display()

# COMMAND ----------

dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

# COMMAND ----------


