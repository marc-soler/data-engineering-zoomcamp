# %%
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# %%
credentials_location = (
    "/Users/msoler/Repos/data-engineering-zoomcamp/google_credentials.json"
)
conf = (
    SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set(
        "spark.jars",
        "/opt/homebrew/Cellar/apache-spark/3.5.0/gcs-connector-hadoop3-2.2.5.jar",
    )
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .set(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        credentials_location,
    )
)

sc = SparkContext(conf=conf)

# %%
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set(
    "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
)
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set(
    "fs.gs.auth.service.account.json.keyfile", "path/to/google_credentials.json"
)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

# %%
spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

# %%
df = spark.read.parquet("gs://spark-to-gcs/fhv/2021/01/")
