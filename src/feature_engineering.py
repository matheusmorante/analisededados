from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

def createFatures(preprocessedData):
    featureCols = ['Quantidade em Estoque', 'Lead Time Total (Dias)', 'mês', 'ano']
    labelCol = 'Quantidade Vendida'

    assembler = VectorAssembler(inputCols=featureCols, outputCol='features')
    transformedDf = assembler.transform(combinedDf)
    finalDf = transformedDf.select('features', labelCol)

    return finalDf
