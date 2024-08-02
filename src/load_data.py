from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Análise de Vendas").getOrCreate()

sales_df = spark.read.csv("hdfs:///user/hadoop/vendas.csv", header=True, inferSchema=True)
products_df = spark.read.csv("hdfs:///user/hadoop/produtos.csv", header=True, inferSchema=True)

vendas_df.show()
produtos_df.show()