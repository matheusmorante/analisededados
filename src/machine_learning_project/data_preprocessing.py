from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import month, year
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

spark = SparkSession.builder \
    .appName("InventoryForecast") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

inventory_df = spark.read.csv("hdfs:///data/inventory.csv", header=True, inferSchema=True)
sales_df = spark.read.csv("hdfs:///data/sales.csv", header=True, inferSchema=True)
leadtime_df = spark.read.csv("hdfs:///data/leadtime.csv", header=True, inferSchema=True)

inventory_df = inventory_df.dropna()
sales_df = sales_df.dropna()
leadtime_df = leadtime_df.dropna()

inventory_df = inventory_df.withColumn("quantity", col("quantity").cast("integer"))
sales_df = sales_df.withColumn("sales_amount", col("sales_amount").cast("float"))
leadtime_df = leadtime_df.withColumn("lead_time", col("lead_time").cast("integer"))

sales_df = sales_df.withColumn("month", month(col("date")))
sales_df = sales_df.withColumn("year", year(col("date")))

combined_df = sales_df.join(inventory_df, on='product_id', how='inner')
combined_df = combined_df.join(leadtime_df, on='product_id', how='left')

combined_df.write.csv("hdfs:///data/processed/combined_data.csv", header=True)
spark.stop()
