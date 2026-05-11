# Databricks notebook source
from datetime import datetime, timedelta
from pyspark.sql.functions import current_date as current_date_func
from pyspark.sql.functions import current_timestamp, current_date, lit, col, from_utc_timestamp, unix_timestamp, expr
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Airline Case Study").getOrCreate()



# COMMAND ----------

bme_silver = spark.table("`workspace`.`dev_silver`.`bme_data`")
flight_silver = spark.table("`workspace`.`dev_silver`.`flight_schedule_data`")
location_silver = spark.table("`workspace`.`dev_silver`.`location_reference_data`")

print(f"BME data: {bme_silver.count()} records")
print(f"Flight schedule data: {flight_silver.count()} records")
print(f"Location reference data: {location_silver.count()} records")

print("\n=== Join 1: BME Usage + Flight Schedule ===")
flight_bme = flight_silver.alias('f').join(
    bme_silver.alias('b'),
    (col('f.airline_id') == col('b.airline_id')) & (col('f.gate_id') == col('b.gate_id')),
    'left'
).select(
    'f.*',  # All columns from flight_silver
    'b.device_id',
    'b.event_start_time',
    'b.event_end_time',
    'b.type_of_event',
    'b.usage_in_mins',
    'b.operation_flag'
)

print(f"Flight-BME joined records: {flight_bme.count()}")
flight_bme.display()

print("\n=== Join 2: Adding Route Information ===")
comprehensive_analysis = flight_bme.alias('fb').join(
    location_silver.alias('l'),
    (col('fb.airline_id') == col('l.airline_id')) & (col('fb.take_off_time') == col('l.take_off_time')),
    'left'
).select(
    'fb.*',  # All columns from flight_bme (which includes all flight + bme columns)
    'l.source',
    'l.destination',
    'l.type_of_flight'
).distinct()

print(f"Comprehensive analysis records: {comprehensive_analysis.count()}")
comprehensive_analysis.display()

print("\n=== Saving comprehensive analysis to dev_gold ===")
comprehensive_analysis.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("`workspace`.`dev_gold`.`comprehensive_analysis`")

print("✓ Comprehensive analysis saved to workspace.dev_gold.comprehensive_analysis")

# COMMAND ----------

# DBTITLE 1,Verify and display gold layer table
print("=== Loading comprehensive_analysis from dev_gold ===")

gold_analysis = spark.table("`workspace`.`dev_gold`.`comprehensive_analysis`")

print(f"✓ Table loaded successfully")
print(f"✓ Total records: {gold_analysis.count()}")
print(f"✓ Total columns: {len(gold_analysis.columns)}")
print(f"\nSchema: {', '.join(gold_analysis.columns)}")

print("\n=== Sample Data from Gold Layer ===")
gold_analysis.display()

# COMMAND ----------

# DBTITLE 1,Export to Excel
# MAGIC %pip install openpyxl -q
# MAGIC
# MAGIC print("\n=== Exporting Comprehensive Analysis to Excel ===")
# MAGIC
# MAGIC if 'gold_analysis' not in locals():
# MAGIC     gold_analysis = spark.table("`workspace`.`dev_gold`.`comprehensive_analysis`")
# MAGIC     print(f"✓ Loaded {gold_analysis.count()} records from gold layer")
# MAGIC
# MAGIC print("\nConverting to Pandas DataFrame...")
# MAGIC pandas_df = gold_analysis.toPandas()
# MAGIC print(f"✓ Converted {len(pandas_df)} rows to Pandas")
# MAGIC
# MAGIC excel_file = "/tmp/comprehensive_analysis.xlsx"
# MAGIC csv_file = "/tmp/comprehensive_analysis.csv"
# MAGIC
# MAGIC print(f"\nExporting to Excel...")
# MAGIC pandas_df.to_excel(excel_file, index=False, sheet_name='Analysis')
# MAGIC print("✓ Excel file created")
# MAGIC
# MAGIC print("\nExporting to CSV...")
# MAGIC pandas_df.to_csv(csv_file, index=False)
# MAGIC print("✓ CSV file created")
# MAGIC
# MAGIC print("\nCopying files to FileStore for download...")
# MAGIC try:
# MAGIC     dbutils.fs.cp("file:" + excel_file, "dbfs:/FileStore/comprehensive_analysis.xlsx", True)
# MAGIC     dbutils.fs.cp("file:" + csv_file, "dbfs:/FileStore/comprehensive_analysis.csv", True)
# MAGIC     print("✓ Files copied to FileStore successfully")
# MAGIC except Exception as e:
# MAGIC     print(f"Note: {e}")
# MAGIC     print("Files are available in /tmp/ directory")
# MAGIC
# MAGIC print("\n" + "="*60)
# MAGIC print("✓ Export completed successfully!")
# MAGIC print("="*60)
# MAGIC print(f"\nFile Details:")
# MAGIC print(f"  Total Rows: {len(pandas_df):,}")
# MAGIC print(f"  Total Columns: {len(pandas_df.columns)}")
# MAGIC print(f"\nFiles created:")
# MAGIC print(f"  1. Excel: {excel_file}")
# MAGIC print(f"  2. CSV: {csv_file}")
# MAGIC print(f"\nTo download:")
# MAGIC print(f"  - Navigate to: Data > Browse DBFS > FileStore")
# MAGIC print(f"  - Or use: dbutils.fs.ls('dbfs:/FileStore/')")
# MAGIC print("="*60)

# COMMAND ----------

# DBTITLE 1,Download Files
import os
from IPython.display import HTML

print("=== Download Instructions ===")
print("\n✓ Files have been created successfully!\n")

# Get file sizes
excel_size = os.path.getsize('/tmp/comprehensive_analysis.xlsx') / 1024 / 1024
csv_size = os.path.getsize('/tmp/comprehensive_analysis.csv') / 1024 / 1024

print("File Details:")
print("="*60)
print(f"  1. Excel File")
print(f"     • Location: /tmp/comprehensive_analysis.xlsx")
print(f"     • Size: {excel_size:.2f} MB")
print(f"     • Format: .xlsx (Microsoft Excel)")
print()
print(f"  2. CSV File")
print(f"     • Location: /tmp/comprehensive_analysis.csv")
print(f"     • Size: {csv_size:.2f} MB")
print(f"     • Format: .csv (Comma Separated Values)")
print("="*60)

print("\nData Summary:")
print(f"  • Total Records: 23,036")
print(f"  • Total Columns: 23")
print(f"  • Includes: Flight operations + BME equipment + Route info")

# Create a styled HTML display
html_display = f"""
<div style="padding: 20px; border: 2px solid #4CAF50; border-radius: 10px; background-color: #f0f8ff; margin-top: 20px;">
    <h2 style="color: #4CAF50;">✅ Export Completed Successfully!</h2>
    <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
        <tr style="background-color: #4CAF50; color: white;">
            <th style="padding: 10px; text-align: left;">File Type</th>
            <th style="padding: 10px; text-align: left;">Location</th>
            <th style="padding: 10px; text-align: left;">Size</th>
        </tr>
        <tr style="background-color: #f9f9f9;">
            <td style="padding: 10px;"><strong>Excel (.xlsx)</strong></td>
            <td style="padding: 10px;"><code>/tmp/comprehensive_analysis.xlsx</code></td>
            <td style="padding: 10px;">{excel_size:.2f} MB</td>
        </tr>
        <tr style="background-color: #ffffff;">
            <td style="padding: 10px;"><strong>CSV (.csv)</strong></td>
            <td style="padding: 10px;"><code>/tmp/comprehensive_analysis.csv</code></td>
            <td style="padding: 10px;">{csv_size:.2f} MB</td>
        </tr>
    </table>
    <div style="margin-top: 20px; padding: 15px; background-color: #fff3cd; border-left: 4px solid #ffc107;">
        <h3 style="margin-top: 0;">How to Download:</h3>
        <ol>
            <li>Files are available in the <code>/tmp/</code> directory on this cluster</li>
            <li>Use the pandas DataFrame variable <code>pandas_df</code> to further analyze the data</li>
            <li>You can also re-export to different locations or formats as needed</li>
        </ol>
    </div>
</div>
"""

displayHTML(html_display)

print("\n" + "="*60)
print("✓ The 'pandas_df' variable is available for further analysis")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Download from Catalog
print("=== Downloading Data from Unity Catalog ===")

# Query the catalog table directly
gold_table = spark.table("`workspace`.`dev_gold`.`comprehensive_analysis`")
print(f"✓ Loaded {gold_table.count():,} records from catalog")

# Convert to Pandas for export
print("\nConverting to Pandas...")
df_pandas = gold_table.toPandas()

# Save to Workspace directory (accessible via file browser)
workspace_dir = "/Workspace/Users/rachit1293@gmail.com/"
excel_path = workspace_dir + "comprehensive_analysis.xlsx"
csv_path = workspace_dir + "comprehensive_analysis.csv"

print(f"\nExporting files to Workspace directory...")
df_pandas.to_excel(excel_path, index=False, sheet_name='Analysis')
print(f"✓ Excel saved: {excel_path}")

df_pandas.to_csv(csv_path, index=False)
print(f"✓ CSV saved: {csv_path}")

print("\n" + "="*70)
print("✅ FILES READY FOR DOWNLOAD!")
print("="*70)
print(f"\n📍 Location: /Users/rachit1293@gmail.com/")
print(f"\n📥 How to download:")
print(f"   1. Click 'Workspace' in the left sidebar")
print(f"   2. Navigate to: Users → rachit1293@gmail.com")
print(f"   3. Right-click on the file → Download")
print(f"\n📊 Files created:")
print(f"   • comprehensive_analysis.xlsx ({len(df_pandas):,} rows)")
print(f"   • comprehensive_analysis.csv ({len(df_pandas):,} rows)")
print("="*70)