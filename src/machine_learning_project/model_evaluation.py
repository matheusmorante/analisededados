from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("Previsão de Vendas - Avaliação do Modelo").getOrCreate()

test_df = spark.read.parquet("hdfs:///user/hadoop/dados_teste.parquet")

model = PipelineModel.load("hdfs:///user/hadoop/modelo_treinado")

predicts = model.transform(test_df)

evaluator = RegressionEvaluator(labelCol="Total", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predicts)
print(f"Root Mean Squared Error (RMSE): {rmse}")
