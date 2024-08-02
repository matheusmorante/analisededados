from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline

# Cria uma sessão Spark
spark = SparkSession.builder.appName("Previsão de Vendas - Pré-processamento").getOrCreate()

# Carrega os dados do HDFS
data_df = spark.read.csv("hdfs:///user/hadoop/vendas.csv", header=True, inferSchema=True)

# Conversão de colunas categóricas para numéricas
indexer = StringIndexer(inputCol="Categoria", outputCol="CategoriaIndex")

# Assembler para combinar colunas de entrada em um vetor de características
assembler = VectorAssembler(
    inputCols=["Quantidade", "Desconto", "CategoriaIndex"],
    outputCol="features"
)

# Preparação do pipeline de pré-processamento
pipeline_prep = Pipeline(stages=[indexer, assembler])
dados_prepared_df = pipeline_prep.fit(dados_df).transform(dados_df)

# Salvar o DataFrame preparado, se necessário
dados_prepared_df.write.parquet("hdfs:///user/hadoop/dados_preparados.parquet")