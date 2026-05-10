#!/usr/bin/env python
# coding: utf-8

# In[7]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from pyspark.sql.functions import input_file_name, lit
import datetime


# In[2]:


# Initialize SparkSession
spark = SparkSession.builder.appName("LandingToBronze_Claims_CPTCodes").getOrCreate()


# In[3]:


# Configuration
GCS_BUCKET = "healthcare-bucket-duynp20"
PROJECT_ID = "duynp20-project"
BRONZE_DATASET = "bronze"
TEMP_GCS_BUCKET = f"{GCS_BUCKET}/temp/"


# In[4]:


# Claims Schema
claims_schema = StructType([
    StructField("claimid", StringType(), True),
    StructField("transactionid", StringType(), True),
    StructField("patientid", StringType(), True),
    StructField("encounterid", StringType(), True),
    StructField("providerid", StringType(), True),
    StructField("deptid", StringType(), True),
    StructField("servicedate", DateType(), True),
    StructField("claimdate", DateType(), True),
    StructField("payorid", StringType(), True),
    StructField("claimamount", DoubleType(), True),
    StructField("paidamount", DoubleType(), True),
    StructField("claimstatus", StringType(), True),
    StructField("payortype", StringType(), True),
    StructField("deductible", DoubleType(), True),
    StructField("coinsurance", DoubleType(), True),
    StructField("copay", DoubleType(), True),
    StructField("insertdate", DateType(), True),
    StructField("modifieddate", DateType(), True)
])


# In[5]:


# Processing Claims data
claims_df = spark.read.csv(
    f"gs://{GCS_BUCKET}/landing/claims/hospital*_claim_data.csv",
    header=True,
    schema=claims_schema,
    dateFormat="yyyy-MM-dd"
)

claims_final = claims_df \
    .withColumn("source_file_path", input_file_name()) \
    .withColumn("extraction_timestamp", lit(datetime.datetime.now()))

claims_final.write.format("bigquery") \
    .option("table", f"{PROJECT_ID}.{BRONZE_DATASET}.claims") \
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
    .mode("overwrite") \
    .save()

print(f"Successfully loaded Claims to {BRONZE_DATASET}.claims")


# In[8]:


# cptcodes Schema
cptcodes_schema = StructType([
    StructField("procedure_code_category", StringType(), True),
    StructField("cpt_codes", IntegerType(), True),
    StructField("procedure_code_descriptions", StringType(), True),
    StructField("code_status", StringType(), True)
])


# In[11]:


# Processing cptcodes
cptcodes_df = spark.read.csv(
    f"gs://{GCS_BUCKET}/landing/cptcodes/cptcodes.csv",
    header=True,
    schema=cptcodes_schema,
    multiLine=True,
    quote="\""
)

cptcodes_final = cptcodes_df.withColumn("extraction_timestamp", lit(datetime.datetime.now()))

cptcodes_final.write.format("bigquery") \
    .option("table", f"{PROJECT_ID}.{BRONZE_DATASET}.cptcodes") \
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
    .mode("overwrite") \
    .save()

print(f"Successfully loaded cptcodes to {BRONZE_DATASET}.claims")


# In[ ]:




