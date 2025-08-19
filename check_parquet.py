from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckParquet").getOrCreate()
df = spark.read.parquet("hdfs://namenode:8020/user/spark/social_sentiment_data/part-00000-0bec0b51-da2b-4e15-992f-1a0402ccb914-c000.snappy.parquet")
df.printSchema()
df.show(5)
spark.stop()