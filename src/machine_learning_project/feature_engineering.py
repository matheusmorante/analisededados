from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder \
    .appName("Feature Engineering") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

combined_df = spark.read.csv("hdfs:///data/processed/combined_data.csv", header=True, inferSchema=True)

feature_cols = ['quantity', 'sales_amount', 'lead_time', 'month', 'year']
label_col = 'demand'

if label_col not in combined_df.columns:
    print(f"Error: Label column '{label_col}' not found in the dataset.")
    spark.stop()
    exit()

assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')

transformed_df = assembler.transform(combined_df)

final_df = transformed_df.select('features', label_col)

final_df.write.csv("hdfs:///data/processed/final_data.csv", header=True)

spark.stop()
