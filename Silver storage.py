# Databricks notebook source
# DBTITLE 1,Importing Libraries
from datetime import datetime, timedelta
from pyspark.sql.functions import current_date as current_date_func
from pyspark.sql.functions import current_timestamp, current_date, lit, col, from_utc_timestamp, unix_timestamp, expr
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Airline Case Study").getOrCreate()


# COMMAND ----------

# DBTITLE 1,Reusable Functions
# Utility function: Convert timestamp columns from CET to IST
def convert_cet_to_ist(dataframe, timestamp_columns):
    """
    Convert timestamp columns from CET (UTC+1) to IST (UTC+5:30)
    
    Parameters:
    - dataframe: PySpark DataFrame
    - timestamp_columns: List of column names to convert
    
    Returns:
    - DataFrame with converted timestamp columns
    """
    df = dataframe
    for col_name in timestamp_columns:
        df = df.withColumn(
            col_name,
            from_utc_timestamp(
                col(col_name).cast("timestamp") - expr("INTERVAL 1 HOUR"),  # Remove CET offset to get UTC
                "Asia/Kolkata"  # Convert to IST
            )
        )
    return df


# COMMAND ----------

# DBTITLE 1,Transform GPU and PCA datasets


gpu_df = spark.sql("""
    SELECT * 
    FROM `workspace`.`dev_bronze`.`gpu_dataset_may2026`
""")
gpu_df = gpu_df.withColumn("type_of_event", lit("GPU"))

print(f"GPU records loaded: {gpu_df.count()}")


pca_df = spark.sql("""
    SELECT * 
    FROM `workspace`.`dev_bronze`.`pca_dataset_may2026`
""")
pca_df = pca_df.withColumn("type_of_event", lit("PCA"))

print(f"PCA records loaded: {pca_df.count()}")


combined_df = gpu_df.unionByName(pca_df)

print(f"Combined records: {combined_df.count()}")


# COMMAND ----------

# DBTITLE 1,Timestamp operations
timestamp_cols = ["event_start_time", "event_end_time"]
combined_df = convert_cet_to_ist(combined_df, timestamp_cols)

combined_df = combined_df.withColumn(
    "usage_in_mins",
    (unix_timestamp(col("event_end_time")) - unix_timestamp(col("event_start_time"))) / 60
)

print("\nTransformations completed:")
print("- Added type_of_event column (GPU/PCA)")
print("- Converted timestamps from CET to IST")
print("- Calculated usage_in_mins")

combined_df.display()

# COMMAND ----------

# DBTITLE 1,Add operation_flag column
combined_df_bme = combined_df.withColumn(
    "operation_flag",
    when(col("airline_id").isNull(), "discarded")
    .when((col("usage_in_mins") >= 5) & (col("usage_in_mins") <= 500), "normal")
    .otherwise("abnormal")
)

print("Operation flag added successfully!")
print(f"Total records: {combined_df_bme.count()}")

combined_df_bme.display()


print("\n=== Saving to dev_silver.bme_data ===")

combined_df_bme.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("`workspace`.`dev_silver`.`bme_data`")

print(" Data successfully saved to workspace.dev_silver.bme_data")
print(f" Total records saved: {combined_df_bme.count()}")

# COMMAND ----------

# DBTITLE 1,Transform flight_schedule_data
print("=== Task 3: Loading and transforming flight_schedule_data ===")

flight_df = spark.sql("""
    SELECT * 
    FROM `workspace`.`dev_bronze`.`flight_schedule_data`
""")

print(f"Flight schedule records loaded: {flight_df.count()}")

timestamp_columns = [
    "estimated_time_of_arrival",
    "estimated_time_of_departure",
    "touch_down_time",
    "take_off_time",
    "on_block_time",
    "off_block_time"
]

flight_df = convert_cet_to_ist(flight_df, timestamp_columns)

print("✓ Timestamps converted from CET to IST")

flight_df = flight_df.withColumn(
    "turnaround_time",
    (unix_timestamp(col("take_off_time")) - unix_timestamp(col("on_block_time"))) / 60
)

print("✓ Turnaround time calculated")

flight_df = flight_df.withColumn(
    "delay_in_arrival",
    (unix_timestamp(col("estimated_time_of_arrival")) - unix_timestamp(col("touch_down_time"))) / 60
)

flight_df = flight_df.withColumn(
    "delay_in_departure",
    (unix_timestamp(col("estimated_time_of_departure")) - unix_timestamp(col("take_off_time"))) / 60
)

print("✓ Delays calculated (arrival and departure)")

flight_df = flight_df.withColumn(
    "delay_in_arrival",
    when(col("delay_in_arrival") < 0, 0).otherwise(col("delay_in_arrival"))
).withColumn(
    "delay_in_departure",
    when(col("delay_in_departure") < 0, 0).otherwise(col("delay_in_departure"))
)

print("✓ Negative delays replaced with 0")

flight_df = flight_df.withColumn(
    "turnaround_flag",
    when((col("turnaround_time") > 600) | (col("turnaround_time") < 10), "investigate")
    .otherwise("normal")
)

print("✓ Turnaround flag added")

print("\nTransformations completed:")
print("- Converted 6 timestamp columns from CET to IST")
print("- Calculated turnaround_time (in minutes)")
print("- Calculated delay_in_arrival (in minutes)")
print("- Calculated delay_in_departure (in minutes)")
print("- Replaced negative delays with 0")
print("- Added turnaround_flag (investigate/normal)")

flight_df.display()

# COMMAND ----------

# DBTITLE 1,Save flight_df to dev_silver
print("=== Saving flight_df to dev_silver.flight_schedule_data ===")

flight_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("`workspace`.`dev_silver`.`flight_schedule_data`")

print("✓ Data successfully saved to workspace.dev_silver.flight_schedule_data")
print(f"✓ Total records saved: {flight_df.count()}")
print(f"✓ Columns: {len(flight_df.columns)}")
print(f"✓ Schema: {', '.join(flight_df.columns)}")

# COMMAND ----------

# DBTITLE 1,Transform location_reference_data
location_df = spark.sql("""
    SELECT * 
    FROM `workspace`.`dev_bronze`.`location_reference_data`
""")

print(f"Location reference records loaded: {location_df.count()}")

timestamp_cols = [field.name for field in location_df.schema.fields 
                  if str(field.dataType) == "TimestampType()"]

print(f"Timestamp columns found: {timestamp_cols}")

if timestamp_cols:
    location_df = convert_cet_to_ist(location_df, timestamp_cols)
    print("✓ Timestamps converted from CET to IST")
else:
    print("✓ No timestamp columns to convert")

print("\n=== Saving to dev_silver.location_reference_data ===")

location_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("`workspace`.`dev_silver`.`location_reference_data`")

print("✓ Data successfully saved to workspace.dev_silver.location_reference_data")
print(f"✓ Total records saved: {location_df.count()}")
print(f"✓ Columns: {len(location_df.columns)}")
print(f"✓ Schema: {', '.join(location_df.columns)}")

location_df.display()