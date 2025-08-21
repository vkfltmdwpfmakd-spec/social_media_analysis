from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DebugSentimentDistribution") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    
    # 데이터 경로
    data_path = "/user/spark/social_sentiment_data"
    
    print(f"Reading data from hdfs://namenode:8020{data_path}...")
    df = spark.read.parquet(data_path)
    
    print("Group by 'sentiment_label' and count:")
    df.groupBy("sentiment_label").count().show(truncate=False)
    
    spark.stop()