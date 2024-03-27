from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

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

# Show the DataFrame with unique key
df_with_key.show()

# Write the DataFrame with unique key back to S3
df_with_key.write.parquet("s3a://bucket/path/to/output_parquet", mode="overwrite")

# Stop SparkSession
spark.stop()
