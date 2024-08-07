from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from sklearn.linear_model import LinearRegression as SklearnLR
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pandas as pd
import numpy as np


def trainModel(features):
    pandasDf = features.toPandas()

    X = np.array(pandas_df['features'].tolist())
    y = pandasDf['Quantidade Vendida'].values

    trainX, testX, trainY, testY = train_test_split(X, y, test_size=0.2, random_state=1234)

    model = SklearnLR()
    model.fit(trainX, trainY)

    predictions = model.predict(testX)

    testDf = pd.DataFrame(testX, columns=['features'])
    testDf['Quantidade Vendida'] = testY
    testDf = spark.createDataFrame(testDf)

    return model, testDf


def makePredictions(model, testDf):
    pandasDf = testDf.toPandas()
    testX = np.array(pandasDf['features'].tolist())

    predictions = model.predict(TestX)

    testDf = testDf.withColumn("Predictions", predictions)

    return testDf
