#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
import json


# In[2]:


# Initialize Google Cloud Storage and BigQuery
storage_client = storage.Client()
bq_client = bigquery.Client()


# In[3]:


# Initialize SparkSession
spark = SparkSession.builder.appName("HospitalBToLanding").getOrCreate()


# In[4]:


# GCS Configuration
GCS_BUCKET = "healthcare-bucket-duynp20"
HOSPITAL_NAME = "hospital-b"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"


# In[5]:


# BigQuery Configuration
BQ_PROJECT = "duynp20-project"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"


# In[6]:


# PostgreSQL Configuration
POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://10.54.16.5:5432/hospital_b_db",
    "driver": "org.postgresql.Driver",
    "user": "duynp20",
    "password": "Nguyenduy@2003"
}


# In[7]:


log_entries = []

def log_event(event_type, message, table = None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")


# In[8]:


# Read Config File
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "Read config file successfully")
    return df


# In[9]:


config_df = read_config_file()
config_df.show()


# In[10]:


# Logs to GCS
def save_logs_to_gcs():
    """Save logs to a JSON file and upload to GCS"""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)

    # Get GCS bucket
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)

    # Upload JSON data as a file
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")


# In[11]:


# Logs to BigQuery
def save_logs_to_bigquery():
    """Save logs to BigQuery"""
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery") \
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()
        print("Logs stored in BigQuery for future analysis")


# In[12]:


# Function to Move Existing Files to Archive
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]
    
    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return
    
    run_ts = datetime.datetime.now().strftime("%H%m%S%f")
    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
        
        # Extract Date from file name
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        filename = file.split("/")[-1]
        
        # Move to Archive
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{run_ts}/{filename}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)
        
        # Copy file to archive and delete original
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()
        
        log_event("INFO", f"Moved {file} to {archive_path}", table=table)


# In[13]:


# Function to get latest watermark from BigQuery audit table
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}' AND datasource = 'hospital_b_db'
    """
    
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        return row.lastest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"


# In[14]:


# Function to extract data from Postgresql and save to GCS
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)
        
        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full" else \
                f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"
        
        df = (spark.read.format("jdbc")
                 .option("url", POSTGRES_CONFIG["url"])
                 .option("user", POSTGRES_CONFIG["user"])
                 .option("password", POSTGRES_CONFIG["password"])
                 .option("driver", POSTGRES_CONFIG["driver"])
                 .option("dbtable", query)
                 .load())
        
        log_event("SUCCESS", f"Successfully extracted data from {table}", table=table)
        
        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"
        
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(df.toPandas().to_json(orient="records", lines=True), content_type="application/json")
        
        log_event("SUCCESS", f"JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)
        
        # Insert audit entry
        audit_df = spark.createDataFrame(
            [("hospital_b_db", table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")],
            ["datasource", "tablename", "load_type", "record_count", "load_timestamp", "status"]
        )
        
        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save())
        
        log_event("SUCCESS", f"Audit log updated for {table}", table=table)
        
    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)


# In[15]:


# Processing data
for row in config_df.collect():
    if row["is_active"] == '1' and row["datasource"] == "hospital_a_db":
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)
        
save_logs_to_gcs()
save_logs_to_bigquery()


# In[ ]:




