
import suppliers
import pandas as pd

lx_capanema = r'C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\LX\LX Geral'
mapper_capanema = r"C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\LX"

suppliers_map = suppliers.SuppliersLX(lx_capanema, mapper_capanema)
suppliers_map._run_pipeline()
df = suppliers_map.df_report
df_error = suppliers_map.df_errors

# lx_newsteel = r'C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\LX\LX Geral'
# mapper_newsteel = r"C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\LX"

# suppliers_map = suppliers.SuppliersLX(lx_newsteel, mapper_newsteel)
# suppliers_map._run_pipeline()
# df = suppliers_map.df_report
# df_error = suppliers_map.df_errors
# print(report.columns)
print(df.loc[df['cwp'].str.contains('017'), ['cwp', 'cod_ativo']].drop_duplicates(keep='first'))
# print(df)

# df.to_excel(r"C:\Users\emman\OneDrive\Documentos\Pasta1.xlsx", index=False)