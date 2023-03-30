from bim_tools import TracerFullReport
tracer = TracerFullReport(r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\Stagging')
df_tracer = tracer.get_report()
print(df_tracer.head())
# print(df_tracer.loc[df_tracer['tag'].str.contains('CF12-2780E', na=False)])