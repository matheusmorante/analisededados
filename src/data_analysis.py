from data_preprocessing import dataPreprocess
from feature_engineering import createFeatures
from model import trainModel
from model import makePredictions

def runDataAnalysis():
    PreprocessedData = dataPreprocess()

    features = create_features(PreprocessedData)

    model, testDf = train_model(features)

    predictions = make_predictions(model, testDf)

    return predictions

