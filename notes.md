# Course notes

## Useful bash commands
- wget [url] to download a file directly.
- head -n 100 [file_path] > [new_file_path] to get the first 100 rows of this file, and save it to another file
- wc -l [file_path] word count, but for lines
- ipconfig getifaddr en0 to know your ip

## Python
- argparse library allows to add arguments in the terminal when running a python script
- python -m http.server to tart a server on a port, and interact with the directory you're in

## Docker
- when creating multiple containers in docker-compose, they become part of the same network automatically
- docker-compose up -d starts in detached mode, which gives back the terminal for further use

## SQL
"""
WHERE NOT EXISTS (
  SELECT 1
  from zones z
  where z."LocationID" = t."PULocationID"
) 
""" rather than
"""
WHERE NOT IN (
  SELECT 1
  from zones z
  where z."LocationID" = t."PULocationID"
) 
"""

## Terraform
- How to let terraform know where the gcp credentials are:
    - In the provider section, add "credentials" with the path
    - export GOOGLE_CREDENTIALS='path'

## DE concepts
- parametrised execution

## Project tips
- Run the dbt build via API, that can be called when exporting data in mage

## Spark and Pyspark
- When reading a csv, the schema is inferred from the data, but it can be specified
- When writing a parquet, the schema is inferred from the data, but it can be specified
- Transformations are not executed right away (lazy), but when an action is called (eager)
  - Transformations are things like select, filter, groupBy, join
  - Actions are things like show, collect, count, write
- Spark submit
- Running a standalone cluster locally:
  - To start a local cluster in standalone mode, go to SPARK_HOME and execute ./sbin/start-master.sh
  - spark://localhost:7077 when creating the spark object
  - To create workers, 

## GCloud
- Upload files to GCS from terminal: gsutil -m cp -r <folder> gs://<bucket-name>
  - -m enables multithreading in case large file, -r for recursive, because nested folders

## Kafka & Data Streaming
- 

