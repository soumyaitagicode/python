from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
# monotonically_increasing_id() function. This function generates a unique ID for each row, even when the DataFrame is
# partitioned across different executors.

# Create SparkSession
spark = SparkSession.builder \
    .appName("Create Unique Key") \
    .getOrCreate()

# Set AWS credentials
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "your_access_key")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "your_secret_key")

# Read Parquet file from S3
df = spark.read.parquet("s3a://bucket/path/to/parquet_file")

# Create a unique key column
df_with_key = df.withColumn("unique_key", monotonically_increasing_id())

# Write the DataFrame with unique key back to S3 with partitioning
df_with_key.write \
    .partitionBy("partition_column") \
    .mode("overwrite") \
    .parquet("s3a://bucket/path/to/output_parquet")

# Stop SparkSession
spark.stop()
