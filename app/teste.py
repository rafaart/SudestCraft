
import suppliers
import pandas as pd

lx_capanema = r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\LX\LX Geral'
mapper_capanema = r"C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\LX\LX-FORNECEDORES-CAPANEMA.xls"

suppliers_map = suppliers.SuppliersLX(lx_capanema, mapper_capanema)
# suppliers_map._read_files()
# suppliers_map.df_errors.to_csv(r'C:\Users\EmmanuelSantana\Documents\teste\error_log.csv', index=False)
# print(suppliers_map.df_errors)
suppliers_map._run_pipeline()
df = suppliers_map.df_report
# print(report.columns)
print(df.loc[df['tag'].str.contains('41-1920CF-04-C232'), ['cwp', 'tag', 'qtd_lx', 'descricao', 'peso_un', 'file_name', 'supplier']])

