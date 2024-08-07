from data_preprocessing import dataPreprocess
from feature_engineering import createFeatures
from model import trainModel, makePredictions

def runDataAnalysis():
    preprocessedData = dataPreprocess()
    features = createFeatures(preprocessedData)

    model, testDf = trainModel(features)
    predictions = makePredictions(model, testDf)

    return predictions

if __name__ == "__main__":
    predictions = runDataAnalysis()
    if predictions:
        predictions.show()
