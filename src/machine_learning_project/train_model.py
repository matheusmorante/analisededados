from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

spark = SparkSession.builder \
    .appName("InventoryForecast") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

combined_df = spark.read.csv("hdfs:///data/processed/combined_data.csv", header=True, inferSchema=True)

assembler = VectorAssembler(
    inputCols=['quantity', 'lead_time'],
    outputCol='features'
)

data_df = assembler.transform(combined_df)

final_df = data_df.select(col('sales_amount').alias('label'), 'features')

train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=1234)

lr = LinearRegression()

pipeline = Pipeline(stages=[assembler, lr])

model = pipeline.fit(train_df)

predictions = model.transform(test_df)

spark.stop()