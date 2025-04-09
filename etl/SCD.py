from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("SCD_Type_2_Example") \
    .config("spark.sql.warehouse.dir", "s3://your-bucket-name/warehouse/") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Define input and output paths
input_path = "s3://your-bucket-name/input-data/"
output_path = "s3://your-bucket-name/output-data/"

# Load the data from the source table
source_data = spark.read.format("parquet").load(input_path)

# Load the current data in the target Hive SCD table
# Assume that target table is already created in Hive and exists in S3
target_table = spark.read.format("hive").table("your_hive_table")

# Define the primary key and change detection logic
primary_key = "id"
effective_date_column = "effective_date"
expiry_date_column = "expiry_date"
current_flag_column = "current_flag"

# Identify changed records
# Create a condition to identify new or changed records
joined_data = source_data.join(
    target_table,
    source_data[primary_key] == target_table[primary_key],
    how="left"
)

# Define a condition to detect changes
changed_data = joined_data.filter(
    (source_data["column_to_check"] != target_table["column_to_check"]) |
    target_table[primary_key].isNull()
)

# Create a dataframe for new rows (current data)
new_rows = changed_data.select(
    source_data["*"],
    current_date().alias(effective_date_column),
    lit(None).cast(StringType()).alias(expiry_date_column),
    lit(True).alias(current_flag_column)
)

# Create a dataframe for expired rows
expired_rows = joined_data.filter(
    source_data[primary_key].isNotNull() &
    (source_data["column_to_check"] != target_table["column_to_check"])
).select(
    target_table["*"],
    current_date().alias(expiry_date_column),
    lit(False).alias(current_flag_column)
)

# Insert the new rows into the target SCD table (Type 2)
new_rows.write \
    .format("hive") \
    .mode("append") \
    .saveAsTable("your_hive_table")

# Update the expired rows to mark them as non-current
expired_rows.write \
    .format("hive") \
    .mode("append") \
    .saveAsTable("your_hive_table")

# Optionally, you can use an SCD library if available in the jar for more advanced handling
# For instance, if you're using a custom SCD jar in Spark, you might configure it like this:
# scd_jar_path = "s3://your-bucket-name/libs/scd_jar.jar"
# spark.sparkContext.addJar(scd_jar_path)
# You can then apply SCD logic using the jar as part of the processing pipeline.

# Commit the transaction and close the session
spark.stop()
