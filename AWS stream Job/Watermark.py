from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Define the schema of the JSON messages
schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True),
    StructField("col3", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Initialize the SparkSession
spark = SparkSession.builder.appName("KafkaStream").getOrCreate()

# Configure the S3 credentials and output path
access_key = os.environ.get('AWS_ACCESS_KEY_ID')
secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
s3_output_path = "s3a://bucket_name/output_path/"

# Define the Kafka input parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "topic_name",
    "startingOffsets": "earliest"
}

# Read the JSON messages from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.security.protocol", "PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='password';") \
    .option("kafka.ssl.truststore.location", "/path/to/truststore") \
    .option("kafka.ssl.truststore.password", "truststore_password") \
    .option("kafka.ssl.endpoint.identification.algorithm", "") \
    .option("failOnDataLoss", "false") \
    .option("checkpointLocation", "/path/to/checkpoint_dir") \
    .option("group.id", "group_id") \
    .option("maxOffsetsPerTrigger", 1000) \
    .options(**kafka_params) \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))

# Apply watermarking to handle late arrivals
watermarked_df = df \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(window("timestamp", "1 hour")) \
    .agg(sum("data.col3").alias("sum_col3"))

# Write the aggregated data to S3
query = watermarked_df \
    .writeStream \
    .format("json") \
    .option("path", s3_output_path) \
    .option("checkpointLocation", "/path/to/checkpoint_dir") \
    .option("awsAccessKey", access_key) \
    .option("awsSecretKey", secret_key) \
    .option("compression", "gzip") \
    .trigger(processingTime='1 hour') \
    .start()

# Wait for the job to terminate
query.awaitTermination()
