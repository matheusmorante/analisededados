from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Train Test Split") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

final_df = spark.read.csv("hdfs:///data/processed/final_data.csv", header=True, inferSchema=True)

train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=1234)

train_df.write.csv("hdfs:///data/processed/train_data.csv", header=True)
test_df.write.csv("hdfs:///data/processed/test_data.csv", header=True)

spark.stop()