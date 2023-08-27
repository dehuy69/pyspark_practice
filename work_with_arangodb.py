from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.types import MapType
from pyspark.sql.functions import col

# Define the schema
schema = StructType([
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

# Print the schema
print(schema)

spark = SparkSession \
    .builder \
    .appName("test") \
    .config("spark.jars.packages", "com.arangodb:arangodb-spark-datasource-3.4_2.12:1.5.0") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

# Read data from ArangoDB into a DataFrame
query_config = {
    "username": "root",
    "password": "test",
    "endpoints": "0.0.0.0:8549,0.0.0.0:8539,0.0.0.0:8529",
    "table": "orders"
}

df = spark.read \
    .format("com.arangodb.spark") \
    .options(**query_config) \
    .schema(schema) \
    .load()

df.show()

# Filter the DataFrame
filtered_df = df.filter(col("itemid") == "Item_1")

# Show the filtered DataFrame
filtered_df.show()

# Stop the Spark session when done
spark.stop()
