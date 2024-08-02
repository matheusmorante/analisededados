from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName("Previsão de Vendas - Previsões").getOrCreate()

modelo = PipelineModel.load("hdfs:///user/hadoop/modelo_treinado")

new_data_df = spark.createDataFrame([
    (5, 0.1, "Móveis"),
    (10, 0.2, "Eletrônicos")
], ["Quantidade", "Desconto", "Categoria"])

pipeline_prep = PipelineModel.load("hdfs:///user/hadoop/pipeline_ml")
new_data_prepared_df = pipeline_prep.transform(new_data__df)

new_predictions = model.transform(new_data_prepared_df)
new_predictions.select("features", "prediction").show()
