from pyspark.sql import SparkSession
from data_preprocessing import preprocess_data
from feature_engineering import engineer_features
from train_test_split import split_data
from train_model import train_model
from evaluate_model import evaluate_model

def main():
    # Inicializar a sessão Spark
    spark = SparkSession.builder \
        .appName("Main Pipeline") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    print("Iniciando o pré-processamento de dados...")
    preprocess_data(spark)

    print("Realizando engenharia de características...")
    engineer_features(spark)

    print("Dividindo os dados em conjuntos de treino e teste...")
    train_df, test_df = split_data(spark)

    print("Treinando o modelo...")
    model = train_model(train_df)

    print("Avaliando o modelo...")
    evaluate_model(spark)

    # Encerrar a sessão Spark
    spark.stop()

if __name__ == "__main__":
    main()