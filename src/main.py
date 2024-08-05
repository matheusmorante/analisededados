from data_processing import dataPreprocess
from feature_engineering import create_features
from train_test_split import train_test_split
from model_training import train_model
from model_evaluation import evaluate_model

if __name__ == "__main__":
    PreprocessedData = dataPreprocess()

    features = create_features(PreprocessedData)

    train_model(features)

    evaluate_model()