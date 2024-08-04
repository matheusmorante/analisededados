from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("InventoryForecast") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

inventory_df = spark.read.csv("hdfs:///data/inventory.csv", header=True, inferSchema=True)
sales_df = spark.read.csv("hdfs:///data/sales.csv", header=True, inferSchema=True)
leadtime_df = spark.read.csv("hdfs:///data/suppliers_lead_time.csv", header=True, inferSchema=True)

inventory_df = inventory_df.dropna()
sales_df = sales_df.dropna()
leadtime_df = leadtime_df.dropna()

inventory_df = inventory_df.withColumn("Quantitidade em Estoque", col("Quantitidade em Estoque").cast("integer"))
sales_df = sales_df.withColumn("Total", col("Total").cast("float"))
leadtime_df = leadtime_df.withColumn("Lead Time Total (Dias)", col("Lead Time Total (Dias)").cast("integer"))

sales_df = sales_df.withColumn("mês", month(col("data")))
sales_df = sales_df.withColumn("ano", year(col("data")))

combined_df = sales_df.join(inventory_df, on='Id do Produto', how='inner')
combined_df = combined_df.join(leadtime_df, on='Id do Produto', how='left')

combined_df.write.csv("hdfs:///data/processed/combined_data.csv", header=True)
spark.stop()
