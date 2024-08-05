from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

def create_features(preprocessedData):
    featureCols = ['Quantidade', 'Quantidade Vendida', 'Lead Time Total (Dias)', 'mês', 'ano']
    labelCol = 'Quantidade Vendida'

    if label_col not in preprocessedData.columns:
        print(f"Error: Label column '{labelCol}' not found in the dataset.")
        spark.stop()
        return

    assembler = VectorAssembler(inputCols=featureCols, outputCol='features')
    transformedDf = assembler.transform(combinedDf)
    finalDf = transformedDf.select('features', labelCol)

    return finalDf
