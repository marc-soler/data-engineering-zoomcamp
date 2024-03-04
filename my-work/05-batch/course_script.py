# %%
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
import pandas as pd


# %%
# ! wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2021-04.parquet

# %%
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# %%
df = spark.read.option("header", "true").parquet("data/fhv_tripdata_2021-04.parquet")

df.show()

# %%
df.schema

# %%
# !head -n 1001 data/fhv_tripdata_2021-04.parquet > data/head.csv
# !head -n 101 data/head.parquet
# !wc -l data/head.csv

# %%
df_pandas = pd.read_parquet("data/fhv_tripdata_2021-04.parquet")
df_pandas = df_pandas.drop(columns=["SR_Flag"])
df_pandas.dtypes

# %%
spark.createDataFrame(df_pandas).schema

# %%
schema = StructType(
    [
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropOff_datetime", TimestampType(), True),
        StructField("PUlocationID", IntegerType(), True),
        StructField("DOlocationID", IntegerType(), True),
        StructField("Affiliated_base_number", StringType(), True),
    ]
)

# %%
df = (
    spark.read.option("header", "true")
    .schema(schema)
    .parquet("data/fhv_tripdata_2021-04.parquet")
)
df = df.drop("PUlocationID", "DOlocationID")
# %%
df = df.repartition(24)
df.write.parquet("data/fhv/2021/01", mode="overwrite")

# %%
df = spark.read.parquet("data/fhv/2021/01")

# %%
df.printSchema()

# %%
df.select("dispatching_base_num").filter(df.dispatching_base_num == "B02975").show()

# %%
# Spark native functions
from pyspark.sql import functions as F

df = df.withColumn("pickup_date", F.to_date("pickup_datetime"))
df = df.withColumn("pickup_hour", F.hour("pickup_datetime"))
df = df.withColumn("dropoff_date", F.to_date("dropoff_datetime"))
df = df.withColumn("dropoff_hour", F.hour("dropoff_datetime"))


# %%
# User defined functions
def custom_function(x):
    return 0


custom_function = F.udf(custom_function, returnType=StringType())

df = df.withColumn("pickup_date_udf", custom_function(df.pickup_datetime))
