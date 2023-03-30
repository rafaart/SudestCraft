import pandas as pd
import os

root = r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\Modelos BIM\Stagging'
for file in os.listdir(root):
    df = pd.read_parquet(os.path.join(root, file))
    df = df.rename(columns={'assembly_id': 'agg_id'})
    df.to_parquet(os.path.join(root, file), index=False)
