from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

def trainModel(features):
    trainDf, testDf = features.randomSplit([0.8, 0.2], seed=1234)

    lr = LinearRegression(featuresCol='features', labelCol='Quantidade Vendida')
    pipeline = Pipeline(stages=[lr])

    model = pipeline.fit(trainDf)
    return model, testDf

def makePredictions(model, testDf):
    predictions = model.transform(testDf)
    return predictions