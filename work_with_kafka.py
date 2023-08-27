from pyspark.sql import SparkSession
from pyspark.sql.functions import count, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import lit


spark = SparkSession \
    .builder \
    .appName("test") \
    .config("spark.jars.packages", "org.apache.kafka:kafka-clients:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

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

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "0.0.0.0:19092,0.0.0.0:29092,0.0.0.0:39092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .option("spark.streaming.kafka.maxRatePerPartition", "50") \
    .load()

def callback(df, df_batch):
    json_df = df.select(from_json("json", kschema).alias("data")) \
        .select("data.*")

    aggregated_df = json_df.groupBy("itemid").agg(count("*").alias("total"))
    aggregated_df.show()

# Print the Kafka messages to the console
# query = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# Write the Kafka messages to a text file
# query = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .format("text") \
#     .outputMode("append") \
#     .option("checkpointLocation", "output") \
#     .option("path", "./") \
#     .start()

# Start the streaming query to consume Kafka messages
query = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json")\
    .writeStream \
    .outputMode("update") \
    .foreachBatch(callback) \
    .start()

query.awaitTermination()


