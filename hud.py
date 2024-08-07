import tkinter as tk
from tkinter import filedialog, messagebox
from pyspark.sql import SparkSession
from data_analysis import runDataAnalysis
import time
import os

class DataAnalysisApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Analysis with PySpark")

        self.salesFile = ""
        self.inventoryFile = ""
        self.leadtimeFile = ""

        self.createWidgets()
        self.spark = self.initializeSpark()

    def createWidgets(self):
        self.salesButton = tk.Button(self.root, text="Selecione o arquivo CSV de vendas", command=self.selectSalesFile)
        self.salesButton.grid(row=0, column=0)

        self.inventoryButton = tk.Button(self.root, text="Selecione o arquivo CSV de estoque", command=self.selectInventoryFile)
        self.inventoryButton.grid(row=1, column=0)

        self.leadtimeButton = tk.Button(self.root, text="Selecione o arquivo CSV de Lead time de fornecedores", command=self.selectLeadtimeFile)
        self.leadtimeButton.grid(row=2, column=0)

        self.startButton = tk.Button(self.root, text="Executar analise", command=self.startAnalysis)
        self.startButton.grid(row=3, column=0, columnspan=2)

    def selectSalesFile(self):
        self.salesFile = filedialog.askopenfilename(title="Seleção de arquivo CSV de vendas", filetypes=[("CSV files", "*.csv")])
        if self.salesFile:
            messagebox.showinfo("Arquivo selecionado", f"Arquivo CSV de vendas: {self.salesFile}")

    def selectInventoryFile(self):
        self.inventoryFile = filedialog.askopenfilename(title="Seleção de arquivo CSV de inventário",
                                                         filetypes=[("CSV file", "*.csv")])
        if self.inventoryFile:
            messagebox.showinfo("Arquivo selecionado", f"Arquivo CSV de estoque: {self.inventoryFile}")

    def selectLeadtimeFile(self):
        self.leadtimeFile = filedialog.askopenfilename(title="Seleção de arquivo CSV de lead time de fornecedores",
                                                        filetypes=[("CSV files", "*.csv")])
        if self.leadtimeFile:
            messagebox.showinfo("Arquivo selecionado", f"Arquivo CSV de lead time: {self.leadtimeFile}")

    def initializeSpark(self):
        return SparkSession.builder \
            .appName("InventoryForecast") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .getOrCreate()

    def startAnalysis(self):
        if not (self.salesFile and self.inventoryFile and self.leadtimeFile):
            messagebox.showerror("Erro", "Selecione todos os arquivos antes de iniciar a análise.")
            return

        try:

            os.system(f"hdfs dfs -put -f {self.salesFile} /data/sales.csv")
            os.system(f"hdfs dfs -put -f {self.inventoryFile} /data/inventory.csv")
            os.system(f"hdfs dfs -put -f {self.leadtimeFile} /data/suppliers_leadtime.csv")

            runDataAnalysis()
            messagebox.showinfo("Success", "Analise Completada com Sucesso!")
        except Exception as e:
            messagebox.showerror("Erro", "Ocorreu um erro")

if __name__ == "__main__":
    root = tk.Tk()
    app = DataAnalysisApp(root)
    root.mainloop()
