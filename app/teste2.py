import pandas as pd
import os

root_path = r"C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\01. Dashboards Ativos\Fornecimento\Codeme\tracer_data.parquet"
# df = pd.ExcelFile(root_path)
df = pd.read_parquet(root_path)
print(df['cwp'].unique())