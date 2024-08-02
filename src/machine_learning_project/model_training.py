from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# Cria uma sessão Spark
spark = SparkSession.builder.appName("Previsão de Vendas - Treinamento do Modelo").getOrCreate()

# Carrega os dados de treino
training_df = spark.read.parquet("hdfs:///user/hadoop/dados_treino.parquet")

# Carrega o pipeline de aprendizado de máquina
pipeline_ml = PipelineModel.load("hdfs:///user/hadoop/pipeline_ml")

# Treinamento do modelo
model = pipeline_ml.fit(treino_df)

# Salvar o modelo treinado
model.write().overwrite().save("hdfs:///user/hadoop/modelo_treinado")
