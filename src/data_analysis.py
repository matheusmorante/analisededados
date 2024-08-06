from data_preprocessing import dataPreprocess
from feature_engineering import createFeatures
from model import trainModel
from model import makePredictions

def runDataAnalysis():
    PreprocessedData = dataPreprocess()

    features = createFeatures(PreprocessedData)

    model, testDf = trainModel(features)

    predictions = makePredictions(model, testDf)

    return predictions

