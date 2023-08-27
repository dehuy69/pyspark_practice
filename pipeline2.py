from pyspark.sql import SparkSession
from pyspark.sql.functions import count, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

'''
Package list:
org.apache.kafka:kafka-clients:3.4.1
org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1
com.arangodb:arangodb-spark-datasource-3.4_2.12:1.5.0
'''

spark = SparkSession \
    .builder \
    .appName("KafkaToArangoDBPipeline") \
    .config("spark.jars.packages",
            "org.apache.kafka:kafka-clients:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.arangodb:arangodb-spark-datasource-3.4_2.12:1.5.0") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

# Define the aschema for the arangodb data
aschema = StructType([
    StructField("itemid", StringType(), True),
    StructField("total", IntegerType(), True),
    StructField("latest_update_timestamp", LongType(), True),
])

# Define ARangoDB configuration
arangoDB_config = {
    "password": "test",
    "endpoints": "0.0.0.0:8549,0.0.0.0:8539,0.0.0.0:8529",
    "table": "orders_agg"
}

# Execute AQL query to aggregate data
preload_df = spark.read \
    .format("com.arangodb.spark") \
    .options(**arangoDB_config) \
    .schema(aschema) \
    .load()
preload_df = preload_df.drop("latest_update_timestamp")

# Define the schema for the kafka data
kschema = StructType([
    StructField("itemid", StringType(), True),
    StructField("address", StructType([
        StructField("zipcode", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("orderid", IntegerType(), True),
    StructField("orderunits", DoubleType(), True),
    StructField("ordertime", LongType(), True)
])

# Read from kafka topic "orders", groupby itemid and agg is sum
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "0.0.0.0:19092,0.0.0.0:29092,0.0.0.0:39092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .option("spark.streaming.kafka.maxRatePerPartition", "50") \
    .load()

# Parse JSON data
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json", "CAST(key AS STRING) as kafka_key") \
    .select(from_json("json", kschema).alias("data"), "kafka_key") \
    .select("data.*", "kafka_key")

aggregated_df = json_df.groupBy("itemid").agg(count("*").alias("total"))

def update_arangodb_collection(df, batch):
    # Query the streaming DataFrame using SQL
    agg_batch_df = spark.sql("SELECT * FROM agg_streaming_data")
    agg_batch_df.show(100)

# Convert streaming DataFrame to batch
query = aggregated_df.writeStream \
    .outputMode("update") \
    .foreachBatch(update_arangodb_collection) \
    .format("memory") \
    .queryName("agg_streaming_data") \
    .trigger(once=True) \
    .start()




# Start the streaming query to consume Kafka messages
# query = aggregated_df.writeStream \
#     .outputMode("update") \
#     .foreachBatch(update_arangodb_collection) \
#     .start()
#
# query.awaitTermination()

# -------------------DEBUG----------------------
# Print the Kafka messages to the console
# query = json_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


# query = batch_df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

# Wait for the query to terminate
query.awaitTermination()
