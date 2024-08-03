from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("Save Model") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

train_data_path = "hdfs:///data/processed/train_data.csv"

train_df = spark.read.csv(train_data_path, header=True, inferSchema=True)

lr = LinearRegression(featuresCol='features', labelCol='demand')
pipeline = Pipeline(stages=[lr])

model = pipeline.fit(train_df)

model_path = "hdfs:///models/lr_model"

model.write().overwrite().save(model_path)

print(f"Model successfully saved to {model_path}")

spark.stop()
