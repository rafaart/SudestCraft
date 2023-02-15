
import suppliers
import pandas as pd

lx_capanema = r'C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\LX\LX Geral'
mapper_capanema = r"C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\LX"

suppliers_map = suppliers.SuppliersLX(lx_capanema, mapper_capanema)
suppliers_map._run_pipeline()
df = suppliers_map.df_report
# print(report.columns)
print(df.loc[df['file_name'].str.contains('DF-ML-0000CF-M-00003-M-00096'), ['descricao', 'peso_un', 'file_name']])

