import tkinter as tk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

def show_graph():
    plt.figure(figsize=(120, 120))
    plt.plot(pandas_df['label_column'], label='Actual')
    plt.plot(pandas_df['prediction'], label='Predicted', linestyle='--')
    plt.legend()
    plt.title('Predicted vs Actual Stock Demand')
    plt.xlabel('Observation')
    plt.ylabel('Demand')
    plt.show()

root = tk.Tk()
root.title("Stock Demand Prediction")

btn = tk.Button(root, text="Show Graph", command=show_graph)
btn.pack()

root.mainloop()