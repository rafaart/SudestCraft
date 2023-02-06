import pandas as pd
import os

root_path = r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\COMENTAR\METSO\Modelos 3D BIM'
dfs = []
for item in os.listdir(root_path):
    if '.xlsx' in item:
        df = pd.read_excel(os.path.join(root_path, item))
        df = df.rename(columns={'Unnamed: 6': 'Parte', 'WEIGHT (KG)': 'Peso'})
        df['arquivo'] = item
        dfs.append(df)

df_merged = pd.concat(dfs)
df_merged.to_excel(r"C:\Users\EmmanuelSantana\Documents\data.xlsx", index=False)
