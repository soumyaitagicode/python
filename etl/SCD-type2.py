from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date

# Create a Spark session with Delta support
spark = SparkSession.builder \
    .appName("SCD_Type_2_Example_Delta") \
    .config("spark.sql.warehouse.dir", "s3://your-bucket-name/warehouse/") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.spark.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# Define paths for input and output
input_path = "s3://your-bucket-name/input-data/"
output_path = "s3://your-bucket-name/output-data/"

# Load source data (new incoming data)
source_data = spark.read.format("parquet").load(input_path)

# Load target data (existing data in the SCD table, stored as Delta table)
target_data = spark.read.format("delta").load(output_path)

# Define the primary key and columns for SCD
primary_key = "id"
effective_date_column = "effective_date"
expiry_date_column = "expiry_date"
current_flag_column = "current_flag"

# Perform the SCD Type 2 merge (insert new records, update changed records, mark old records as expired)
from delta.tables import DeltaTable

# Convert the target DataFrame to a DeltaTable
delta_table = DeltaTable.forPath(spark, output_path)

# Define the merge condition (join based on primary key)
merge_condition = (target_data[primary_key] == source_data[primary_key])

# Perform the merge operation (upsert)
delta_table.alias("target").merge(
    source_data.alias("source"),
    merge_condition
).whenMatchedUpdate(
    condition=(target_data[primary_key] == source_data[primary_key]) & 
              (target_data["column_to_check"] != source_data["column_to_check"]) & 
              (target_data[current_flag_column] == lit(True)),  # Only update active records
    set={
        "expiry_date": current_date(),  # Set expiry date on the existing record
        "current_flag": lit(False)  # Mark the existing record as expired
    }
).whenNotMatchedInsert(
    condition=(source_data["column_to_check"].isNotNull()),
    values={
        "id": source_data[primary_key],
        "column_to_check": source_data["column_to_check"],  # Add other columns as needed
        "effective_date": current_date(),
        "expiry_date": lit(None).cast("date"),
        "current_flag": lit(True)
    }
).execute()

# Write the merged data back to the Delta table (this ensures the changes are persisted)
delta_table.toDF().write.format("delta").mode("overwrite").save(output_path)

# Stop the Spark session
spark.stop()
