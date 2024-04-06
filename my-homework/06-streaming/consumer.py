# %%
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = (
    SparkSession.builder.master("local[*]")
    .appName("GreenTripsConsumer")
    .config("spark.jars.packages", kafka_jar_package)
    .getOrCreate()
)

# %%
green_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "green-trips")
    .option("startingOffsets", "earliest")
    .load()
)


# %%
def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])


query = green_stream.writeStream.foreachBatch(peek).start()
query.stop()

# %%
schema = (
    types.StructType()
    .add("lpep_pickup_datetime", types.StringType())
    .add("lpep_dropoff_datetime", types.StringType())
    .add("PULocationID", types.IntegerType())
    .add("DOLocationID", types.IntegerType())
    .add("passenger_count", types.DoubleType())
    .add("trip_distance", types.DoubleType())
    .add("tip_amount", types.DoubleType())
)
green_stream = green_stream.select(
    F.from_json(F.col("value").cast("STRING"), schema).alias("data")
).select("data.*")


# %%
def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])


query = green_stream.writeStream.foreachBatch(peek).start()
query.stop()

# %%
popular_destinations = (
    green_stream.withColumn("timestamp", F.current_timestamp())
    .groupBy(F.window(F.col("timestamp"), "5 minutes"), "DOLocationID")
    .agg(F.count("passenger_count").alias("count_records"))
    .orderBy(F.desc("count_records"))
)

# %%
query = (
    popular_destinations.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
