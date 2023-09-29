from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import col, rank, window


spark = SparkSession.builder.appName("NYC311toElasticsearch").getOrCreate()

url = "https://data.cityofnewyork.us/resource/fhrw-4uyv.csv?$limit=500000"
df = spark.read.csv(url, header=True, inferSchema=True)

windowSpec = window.Window.orderBy(col("count").desc())
top_complaints = df.groupBy("complaint_type").count().withColumn("rank", rank().over(windowSpec)).filter(col("rank") <= 10)

df = df.join(top_complaints, ["complaint_type"], "left_outer").withColumn("is_top_complaint", col("rank").isNotNull())


# Establish a connection
es = Elasticsearch()

# Configuration for Elasticsearch
es_write_conf = {
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.resource": "nyc_311_data/docs",
    "es.input.json": "true",
    "es.mapping.id": "unique_key"  # 'unique_key' column in the dataset as the identifier
}

# Write DataFrame to Elasticsearch
df.write.format("org.elasticsearch.spark.sql").options(**es_write_conf).mode("overwrite").save()

