from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InventoryForecast") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

inventory_df = spark.read.csv("hdfs:///data/inventory.csv", header=True, inferSchema=True)
sales_df = spark.read.csv("hdfs:///data/sales.csv", header=True, inferSchema=True)
leadtime_df = spark.read.csv("hdfs:///data/leadtime.csv", header=True, inferSchema=True)