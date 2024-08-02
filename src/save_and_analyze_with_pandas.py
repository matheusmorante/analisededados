import pandas as pd

# Lê os resultados salvos no HDFS (supondo que você os tenha copiado para o sistema local)
total_sales_pd = pd.read_csv("/dados/vendas_totais.csv")
average_discountpd = pd.read_csv("/dados/desconto_medio.csv")
low_stock_pd = pd.read_csv("/dados/baixo_estoque.csv")

# Exemplo de análise com Pandas
print(total_sales_pd.head())
print(average_discount_pd.head())
print(low_stock_pd.head())