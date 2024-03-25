# %%
import pyspark
from pyspark.sql import SparkSession

# %%
spark = (
    SparkSession.builder.master("spark://localhost:7077").appName("test").getOrCreate()
)

# %%
df = spark.read.parquet("data/fhv/2021/01")

# %%
df.show()
# %%
df
