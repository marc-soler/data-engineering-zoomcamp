# %%
import json
import time
from kafka import KafkaProducer
import pandas as pd


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


server = "localhost:9092"
cols = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
]
# %%
producer = KafkaProducer(bootstrap_servers=[server], value_serializer=json_serializer)

producer.bootstrap_connected()

# %%
t0 = time.time()

topic_name = "test-topic"

for i in range(10):
    message = {"number": i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

producer.flush()

t1 = time.time()
print(f"took {(t1 - t0):.2f} seconds")

# %%
df_green = pd.read_csv("green_tripdata_2019-10.csv.gz")
df_green = df_green.loc[:, cols]

# %%
t0 = time.time()
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send("green-trips", value=row_dict)

producer.flush()

t1 = time.time()
print(f"took {(t1 - t0):.2f} seconds")

# %%
