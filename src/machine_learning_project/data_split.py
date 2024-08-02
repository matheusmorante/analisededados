from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder.appName("Previsão de Vendas - Divisão de Dados").getOrCreate()

# Carrega os dados pré-processados
data_prepared_df = spark.read.parquet("hdfs:///user/hadoop/dados_preparados.parquet")

# Divisão dos dados em conjuntos de treino e teste
training_df, test_df = data_prepared_df.randomSplit([0.8, 0.2], seed=1234)

# Salvar os DataFrames de treino e teste
training_df.write.parquet("hdfs:///user/hadoop/dados_treino.parquet")
test_df.write.parquet("hdfs:///user/hadoop/dados_teste.parquet")
