from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Análise de Vendas").getOrCreate()

vendas_df = spark.read.csv("hdfs:///user/hadoop/vendas.csv", header=True, inferSchema=True)
produtos_df = spark.read.csv("hdfs:///user/hadoop/produtos.csv", header=True, inferSchema=True)

vendas_df.show()
produtos_df.show()