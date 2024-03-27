from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Join Example") \
    .getOrCreate()

def read_data_from_s3_with_credentials(bucket, path, access_key, secret_key):
    """
    Read data from S3 bucket with specified credentials.
    """
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

    return spark.read.csv(f"s3a://{bucket}/{path}", header=True)

# Read data from different S3 buckets with different credentials
df1 = read_data_from_s3_with_credentials("bucket1", "path/to/data1.csv", "access_key1", "secret_key1")
df2 = read_data_from_s3_with_credentials("bucket2", "path/to/data2.csv", "access_key2", "secret_key2")

# Perform join operation
joined_df = df1.join(df2, "common_column")

# Show the result
joined_df.show()

# Stop SparkSession
spark.stop()
