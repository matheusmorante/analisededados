from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegressionModel

def evaluate_model(testDf, model):
    spark = SparkSession.builder \
        .appName("Evaluate Model") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    model = LinearRegressionModel.load(model)

    predictions = model.transform(testDf)
    predictions.select("features", "Quantidade Vendida", "prediction").show(5)

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
