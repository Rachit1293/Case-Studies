# Databricks notebook source
# DBTITLE 1,Import of Libraries
from datetime import datetime, timedelta
from pyspark.sql.functions import current_date as current_date_func
from pyspark.sql.functions import current_timestamp, current_date
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Airline Case Study").getOrCreate()


# COMMAND ----------

# DBTITLE 1,Flight Schedule data
# Incremental Load: Get max date from target table and load only new records

try:
    max_date_df = spark.sql("""
        SELECT MAX(DATE(estimated_time_of_departure)) as max_date 
        FROM `workspace`.`dev_bronze`.`flight_schedule_data`
    """)
    max_date = max_date_df.collect()[0]['max_date']
    print(f"Maximum date in target table: {max_date}")
except Exception as e:
    # If table doesn't exist or is empty, set max_date to a very old date
    print(f"Target table empty or doesn't exist: {e}")
    max_date = None

if max_date:
    flight_schedule_df = spark.sql(f"""
        SELECT * 
        FROM `workspace`.`default`.`flight_schedule_data`
        WHERE DATE(estimated_time_of_departure) > '{max_date}'
    """)
    print(f"Loading records with estimated_time_of_departure > {max_date}")
else:
    flight_schedule_df = spark.sql("""
        SELECT * 
        FROM `workspace`.`default`.`flight_schedule_data`
    """)
    print("Loading all records (full load)")

flight_schedule_df = flight_schedule_df.withColumn("DataLoadDate", current_date())

try:
    print(f"Records to be loaded: {flight_schedule_df.count()}")
    flight_schedule_df.display()
    flight_schedule_df.write.mode("append").option("mergeSchema", "true").saveAsTable("workspace.dev_bronze.flight_schedule_data")
except Exception as e:
    print("Error in calculating flight schedules data: {e}")

# COMMAND ----------

# DBTITLE 1,Daily GPU Backup - Store with date suffix
# Daily Backup: Store data in dev_bronze_bkp with date suffix
# Creates a new table for each day: tablename_YYYYMMDD

current_date_str = datetime.now().strftime("%Y%m%d")
table_name_daily = f"gpu_dataset_{current_date_str}"

print(f"Creating daily backup table: workspace.dev_bronze_bkp.{table_name_daily}")


gpu_daily = spark.sql("""
    SELECT * 
    FROM `workspace`.`default`.`gpu_dataset`
""")

gpu_daily = gpu_daily.withColumn("DataLoadDate", current_date_func())
print(f"Records to be backed up: {gpu_daily.count()}")


gpu_daily.write.mode("overwrite").saveAsTable(f"workspace.dev_bronze_bkp.{table_name_daily}")

print(f"Daily backup completed: workspace.dev_bronze_bkp.{table_name_daily}")

# COMMAND ----------

# DBTITLE 1,Monthly GPU Aggregation - Store month wise
# Monthly Aggregation: Store data month-wise in dev_bronze

yesterday = datetime.now() - timedelta(days=1)
month_name = yesterday.strftime("%B").lower()  # e.g., "may"
year = yesterday.strftime("%Y")  # e.g., "2026"
month_year = f"{month_name}{year}"  # e.g., "may2026"

table_name_monthly = f"gpu_dataset_{month_year}"

print(f"Data date: {yesterday.strftime('%Y-%m-%d')}")
print(f"Monthly aggregation table: workspace.dev_bronze.{table_name_monthly}")

gpu_dataset_monthly = spark.sql("""
    SELECT * 
    FROM `workspace`.`default`.`gpu_dataset`
""")

gpu_dataset_monthly = gpu_dataset_monthly.withColumn("DataLoadDate", current_date_func())

print(f"Records to be added: {gpu_dataset_monthly.count()}")

gpu_dataset_monthly.write.mode("append").option("mergeSchema", "true").saveAsTable(f"workspace.dev_bronze.{table_name_monthly}")
print(f"Monthly aggregation completed: workspace.dev_bronze.{table_name_monthly}")

# COMMAND ----------

# DBTITLE 1,Daily PCA Backup - Store with date suffix
# Daily Backup: Store data in dev_bronze_bkp with date suffix
# Creates a new table for each day: tablename_YYYYMMDD

current_date_str = datetime.now().strftime("%Y%m%d")
table_name_daily = f"pca_dataset_{current_date_str}"

print(f"Creating daily backup table: workspace.dev_bronze_bkp.{table_name_daily}")

pca_daily = spark.sql("""
    SELECT * 
    FROM `workspace`.`default`.`pca_dataset`
""")

pca_daily = pca_daily.withColumn("DataLoadDate", current_date_func())

print(f"Records to be backed up: {pca_daily.count()}")

pca_daily.write.mode("overwrite").saveAsTable(f"workspace.dev_bronze_bkp.{table_name_daily}")
print(f"Daily backup completed: workspace.dev_bronze_bkp.{table_name_daily}")

# COMMAND ----------

# DBTITLE 1,Monthly PCA Aggregation - Store month wise
# Monthly Aggregation: Store data month-wise in dev_bronze

yesterday = datetime.now() - timedelta(days=1)
month_name = yesterday.strftime("%B").lower()  # e.g., "may"
year = yesterday.strftime("%Y")  # e.g., "2026"
month_year = f"{month_name}{year}"  # e.g., "may2026"

table_name_monthly = f"pca_dataset_{month_year}"

print(f"Data date: {yesterday.strftime('%Y-%m-%d')}")
print(f"Monthly aggregation table: workspace.dev_bronze.{table_name_monthly}")

pca_dataset_monthly = spark.sql("""
    SELECT * 
    FROM `workspace`.`default`.`pca_dataset`
""")

pca_dataset_monthly = pca_dataset_monthly.withColumn("DataLoadDate", current_date_func())

print(f"Records to be added: {pca_dataset_monthly.count()}")

pca_dataset_monthly.write.mode("append").option("mergeSchema", "true").saveAsTable(f"workspace.dev_bronze.{table_name_monthly}")
print(f"Monthly aggregation completed: workspace.dev_bronze.{table_name_monthly}")

# COMMAND ----------

# DBTITLE 1,Location Reference Data
location_ref_df = spark.sql("""
    SELECT * 
    FROM `workspace`.`default`.`location_reference_data`
""")

location_ref_df = location_ref_df.withColumn("DataLoadDate", current_date_func())

print(f"Records loaded: {location_ref_df.count()}")

location_ref_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.dev_bronze.location_reference_data")