from pyspark.ml.feature import VectorAssembler

def createFeatures(preprocessedData):
    featureCols = ['Quantidade em Estoque', 'Lead Time Total (Dias)', 'mês', 'ano']
    labelCol = 'Quantidade Vendida'

    # Criar vetores de características
    assembler = VectorAssembler(inputCols=featureCols, outputCol='features')
    transformedDf = assembler.transform(preprocessedData)
    finalDf = transformedDf.select('features', labelCol)

    return finalDf
