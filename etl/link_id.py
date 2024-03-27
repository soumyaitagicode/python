from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import psycopg2

# Create SparkSession
spark = SparkSession.builder \
    .appName("Write to Parquet and Insert into PostgreSQL") \
    .getOrCreate()

# Read your data
# Replace the 'path' with your actual S3 path
df = spark.read.csv("s3a://your_bucket/your_file.csv", header=True)

# Add a unique ID to each record
df_with_id = df.withColumn("unique_id", monotonically_increasing_id())

# Write DataFrame to Parquet file in S3
# Replace the 'path' with your desired S3 path for the Parquet file
df_with_id.write.parquet("s3a://your_bucket/parquet_output/", mode="overwrite")

# Collect DataFrame to driver node to ensure unique IDs are maintained
collected_df = df_with_id.collect()

# Insert records with unique IDs into PostgreSQL table
conn = psycopg2.connect(
    dbname="your_dbname",
    user="your_username",
    password="your_password",
    host="your_host",
    port="your_port"
)
cur = conn.cursor()
for record in collected_df:
    cur.execute("INSERT INTO exceptions (unique_id, column1, column2) VALUES (%s, %s, %s)",
                (record['unique_id'], record['column1'], record['column2']))
conn.commit()
conn.close()

# Stop SparkSession
spark.stop()
