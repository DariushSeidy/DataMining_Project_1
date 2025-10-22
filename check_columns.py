import pandas as pd
import os

# Quick diagnostic to check column names
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "Online Retail.xlsx")

try:
    df = pd.read_excel(file_path)
    print("Column names in the Excel file:")
    for i, col in enumerate(df.columns):
        print(f"{i+1}. '{col}'")

    print(f"\nFirst few rows:")
    print(df.head())

except Exception as e:
    print(f"Error: {e}")
