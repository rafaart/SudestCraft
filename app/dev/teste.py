import os
import pandas as pd

root = r'C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\01. Dashboards Ativos\Modelo Federado\data'
for item in os.listdir(root):
    if item == '04_assets.csv':
        df = pd.read_csv(os.path.join(root, item))
        print(df)