from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("Análise de Vendas").getOrCreate()

vendas_df = spark.read.csv("hdfs:///user/hadoop/vendas.csv", header=True, inferSchema=True)
produtos_df = spark.read.csv("hdfs:///user/hadoop/produtos.csv", header=True, inferSchema=True)

vendas_totais_df = vendas_df.groupBy("Nome do Produto").agg(sum("Total").alias("Total Vendas"))
vendas_totais_df.write.csv("hdfs:///user/hadoop/vendas_totais.csv", header=True)

desconto_medio_df = vendas_df.groupBy("Nome do Produto").agg((sum("Desconto") / sum("Quantidade")).alias("Desconto Médio"))
desconto_medio_df.write.csv("hdfs:///user/hadoop/desconto_medio.csv", header=True)

juncao_df = vendas_df.join(produtos_df, "Nome do Produto")
baixo_estoque_df = juncao_df.filter(col("Quantidade em Estoque") < 10)
baixo_estoque_df.write.csv("hdfs:///user/hadoop/baixo_estoque.csv", header=True)