from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("Análise de Vendas").getOrCreate()

sales_df = spark.read.csv("hdfs:///user/hadoop/vendas.csv", header=True, inferSchema=True)
products_df = spark.read.csv("hdfs:///user/hadoop/produtos.csv", header=True, inferSchema=True)

total_sales_df = sales_df.groupBy("Nome do Produto").agg(sum("Total").alias("Total Vendas"))
total_sales_df.write.csv("hdfs:///user/hadoop/vendas_totais.csv", header=True)

average_discount_df = sales_df.groupBy("Nome do Produto").agg((sum("Desconto") / sum("Quantidade")).alias("Desconto Médio"))
average_discount_df.write.csv("hdfs:///user/hadoop/desconto_medio.csv", header=True)

joint_df = sales_df.join(produtos_df, "Nome do Produto")
low_stock_df = joint_df.filter(col("Quantidade em Estoque") < 10)
low_stock_df.write.csv("hdfs:///user/hadoop/baixo_estoque.csv", header=True)