from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

# Cria uma sessão Spark
spark = SparkSession.builder.appName("Previsão de Vendas - Construção do Pipeline").getOrCreate()

# Modelo de regressão linear
lr = LinearRegression(featuresCol="features", labelCol="Total")

# Pipeline de aprendizado de máquina
pipeline_ml = Pipeline(stages=[lr])

# Salvar o pipeline treinado, se necessário
pipeline_ml.write().overwrite().save("hdfs:///user/hadoop/pipeline_ml")
