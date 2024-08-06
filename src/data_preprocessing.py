from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year

def dataPreprocess():
    spark = SparkSession.builder \
        .appName("InventoryForecast") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    inventory_df = spark.read.csv("hdfs:///data/inventory.csv", header=True, inferSchema=True)
    salesDf = spark.read.csv("hdfs:///data/sales.csv", header=True, inferSchema=True)
    leadtimeDf = spark.read.csv("hdfs:///data/suppliers_leadtime.csv", header=True, inferSchema=True)

    inventoryDf = inventoryDf.dropna()
    salesDf = salesDf.dropna()
    leadtimeDf = leadtimeDf.dropna()

    inventoryDf = inventoryDf.withColumn("Quantidade em Estoque", col("Quantidade em Estoque").cast("integer"))
    inventoryDf = inventoryDf.withColumn("Quantidade Vendida", col("Quantidade Vendida").cast("integer"))
    leadtimeDf = Df.withColumn("Lead Time Total (Dias)", col("Lead Time Total (Dias)").cast("integer"))

    salesDf = salesDf.withColumn("mês", month(col("data")))
    salesDf = salesDf.withColumn("ano", year(col("data")))

    combinedDf = salesDf.join(inventoryDf, on='Id do Produto', how='inner')
    combinedDf = combinedDf.join(leadtimeDf, on='Id do Produto', how='left')

    return combinedDf
    spark.stop()

