from pyspark.sql import SparkSession

# Function to create SparkSession with custom AWS access keys
# Regarding connecting to multiple S3 buckets with different AWS access keys within the same Spark application,
#  while you cannot directly set different AWS access keys per connection or per bucket within the same Spark application,
#   you can dynamically change the AWS access keys within the same Spark application by resetting the Hadoop configuration,
#   allowing you to access different
#  S3 buckets with different access keys at different times during the execution of your Spark application.



# Function to create SparkSession with custom AWS access keys
def create_spark_session(access_key, secret_key):
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    return spark

# Create SparkSession with custom AWS access keys
spark = create_spark_session("your_access_key", "your_secret_key")

# Read data from first S3 bucket
df1 = spark.read.parquet("s3a://bucket1/path/to/parquet1")

# Your processing logic for df1 goes here

# Reset AWS access keys for the second S3 bucket
spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3a.access.key")
spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3a.secret.key")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "another_access_key")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "another_secret_key")

# Read data from second S3 bucket
df2 = spark.read.parquet("s3a://bucket2/path/to/parquet2")

# Your processing logic for df2 goes here

# Broadcast join dataframes from different buckets
joined_df = df1.join(df2.hint("broadcast"), df1.some_key == df2.some_key)

# Stop SparkSession
spark.stop()
