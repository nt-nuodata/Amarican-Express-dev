# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from faker import Faker
import re
import random

# COMMAND ----------

fake = Faker()
date=['DT','DATE','TSTMP','SESSION']
number=['ID','NBR','CD','MO','IND']
amt=['AMT','PRICE','COST','QTY','CNT','RATE','PCT']
dog_names=['Ember','Tokyo','Heaven','Tuffy','Barkey','Oscar']
TYPE=['Rabbit','Horse','Bird','Rat','Ferret','Fish']
breed=['Arab','Californian','Catfishes','Sparrow','Catfishes']
allergy=['Sneezing','Cough','Red-eyes']
med_cond=['Distemper','Heartworms','Allergies','SkinInfection']
txn=['Cash','Debit-Card','Paytm']
brand= ['Orijen','Nestlé','Mammoth']

# COMMAND ----------

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

file_location = dbutils.widgets.get('file_location')
no_of_rows = dbutils.widgets.get('no_of_rows')

# COMMAND ----------

with open(file_location) as F:
    contents = F.read()
cont = contents.split(";\n")
if re.search("DATABASE", cont[0]) != None:
    cont.pop(0)  
for ddl in cont:
    spark.sql(f"""{ddl}""")

# COMMAND ----------

def find_table_name(text):
    left = "CREATE TABLE IF NOT EXISTS"
    result = re.search('%s(.*)' % (left), text).group(1).split('(')[0]
    return result

# COMMAND ----------

tables = []
for val in cont:
    table_name = find_table_name(val)
    tables.append(table_name)

# COMMAND ----------

for table in tables:
    df = spark.sql(f"""select * from {table}""")
    df1 = spark.range(no_of_rows).withColumn('id', lit(0).cast(IntegerType()))
    for col in df.dtypes:
        df1=df1.withColumn(col[0],generate_random_data_udf(lit(col[0])))
        df1=df1.drop("id")
        df1.createOrReplaceTempView(table.replace('.','_'))

# COMMAND ----------


