import pandas as pd

# Lê os resultados salvos no HDFS (supondo que você os tenha copiado para o sistema local)
vendas_totais_pd = pd.read_csv("/dados/vendas_totais.csv")
desconto_medio_pd = pd.read_csv("/dados/desconto_medio.csv")
baixo_estoque_pd = pd.read_csv("/dados/local/baixo_estoque.csv")

# Exemplo de análise com Pandas
print(vendas_totais_pd.head())
print(desconto_medio_pd.head())
print(baixo_estoque_pd.head())