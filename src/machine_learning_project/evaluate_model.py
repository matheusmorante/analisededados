from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegressionModel

spark = SparkSession.builder \
    .appName("Evaluate Model") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

test_data_path = "hdfs:///data/processed/test_data.csv"
model_path = "hdfs:///models/lr_model"

test_df = spark.read.csv(test_data_path, header=True, inferSchema=True)

lr_model = LinearRegressionModel.load(model_path)

predictions = lr_model.transform(test_df)

predictions.select("features", "demand", "prediction").show(5)

evaluator = RegressionEvaluator(labelCol="demand", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

r2_evaluator = RegressionEvaluator(labelCol="demand", predictionCol="prediction", metricName="r2")
r2 = r2_evaluator.evaluate(predictions)
print(f"R^2 on test data = {r2}")

mae_evaluator = RegressionEvaluator(labelCol="demand", predictionCol="prediction", metricName="mae")
mae = mae_evaluator.evaluate(predictions)
print(f"Mean Absolute Error (MAE) on test data = {mae}")

spark.stop()
