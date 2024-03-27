from pyspark.sql import SparkSession

# Function to create SparkSession with custom AWS access keys
# Regarding connecting to multiple S3 buckets with different AWS access keys within the same Spark application,
#  while you cannot directly set different AWS access keys per connection or per bucket within the same Spark application,
#   you can dynamically change the AWS access keys within the same Spark application by resetting the Hadoop configuration,
#   allowing you to access different
#  S3 buckets with different access keys at different times during the execution of your Spark application.

def create_spark_session(access_key, secret_key):
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    return spark

# Create SparkSession for first S3 bucket
spark1 = create_spark_session("access_key1", "secret_key1")
df1 = spark1.read.parquet("s3a://bucket1/path/to/parquet1")

# Your processing logic for bucket1 goes here

# Create SparkSession for second S3 bucket
spark2 = create_spark_session("access_key2", "secret_key2")
df2 = spark2.read.parquet("s3a://bucket2/path/to/parquet2")

# Your processing logic for bucket2 goes here

# Join dataframes from different buckets
joined_df = df1.join(df2, df1.some_key == df2.some_key)

# Stop SparkSessions
spark1.stop()
spark2.stop()
