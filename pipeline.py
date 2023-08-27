from pyspark.sql import SparkSession
from pyspark.sql.functions import count, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import lit


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

# Global DataFrame for caching
preload_df = None

# Execute AQL query to aggregate data
# Query the database and cache the DataFrame
if preload_df is None:
    preload_df = spark.read \
        .format("com.arangodb.spark") \
        .options(**arangoDB_config) \
        .schema(aschema) \
        .load()
    preload_df = preload_df.withColumn("total", col("total").cast("int"))
    preload_df.cache()

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

# Define a function to update the arangodb collection
def save_df(ps_df, table_name: str, options: dict[str, str], table_type: str = None) -> None:
    if not table_type:
        table_type = "document"

    all_opts = {
        **options,
        "table.shards": "1",
        "confirmTruncate": "true",
        "overwriteMode": "update",
        "table": table_name,
        "table.type": table_type
    }

    ps_df.write\
        .mode("overwrite")\
        .format("com.arangodb.spark")\
        .options(**all_opts)\
        .save()
def update_arangodb_collection(df, batch_id):
    # Execute the SQL query to update the table
    config = {
        **arangoDB_config,
        "overwriteMode": "replace",
    }
    df.write \
        .format("com.arangodb.spark") \
        .mode("append") \
        .options(**config).save()

    # save_df(df, table_name="orders_agg", options=arangoDB_config)
def convert_update(df, batch_id):
    df = df.withColumnRenamed("total", "total_1")
    merged_df = preload_df.join(df, on="itemid", how="inner")

    updated_df = merged_df.withColumn(
        "total",
        expr("total + total_1").alias("total")
    ).withColumn(
        "latest_update_timestamp",
        current_timestamp().cast("long").alias("latest_update_timestamp")
    )
    final_df = updated_df.drop("total_1")
    final_df = final_df.withColumn("_key", df["itemid"].alias("_key"))

    df.show()
    # preload_df.show()
    # merged_df.show()
    # final_df.show()

    # Execute the AQL query to update the table
    config = {
        **arangoDB_config,
        "overwriteMode": "replace",
    }
    final_df.write \
        .format("com.arangodb.spark") \
        .mode("append") \
        .options(**config).save()


def convert_update2(df, batch_id):
    # Add latest_update_timestamp to stream df
    df = df.withColumn("latest_update_timestamp", lit(None).cast("long"))

    # Step 1: update preload_df với giá trị mới từ stream df
    global preload_df
    # Union the two DataFrames to combine the data
    combined_df = preload_df.unionAll(df)
    combined_df.show()
    # Calculate the total sum for each item using groupBy and agg
    # result_df = combined_df.groupBy("itemid").agg(
    #     _sum("total").alias("total"),
    #     current_timestamp().cast("long").alias("latest_update_timestamp")
    # )
    # preload_df = result_df

    # Part 2: Lấy phần giao df và preload_df
    # filtered_df = preload_df.join(df, on="itemid", how="inner").withColumn(
    #     "latest_update_timestamp",
    #     current_timestamp().cast("long").alias("latest_update_timestamp")
    # )

    # Add _key
    # final_df = filtered_df.withColumn("_key", filtered_df["itemid"].alias("_key"))


    # Execute the AQL query to update the table
    # config = {
    #     **arangoDB_config,
    #     "overwriteMode": "replace",
    # }
    # final_df.write \
    #     .format("com.arangodb.spark") \
    #     .mode("append") \
    #     .options(**config).save()

# Start the streaming query to consume Kafka messages
query = aggregated_df.writeStream \
    .outputMode("update") \
    .foreachBatch(convert_update2) \
    .start()

query.awaitTermination()

# -------------------DEBUG----------------------
# Print the Kafka messages to the console
# query = json_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


# query = aggregated_df.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .start()

# Wait for the query to terminate
# query.awaitTermination()
