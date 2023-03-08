from bim_tools import TracerFullReport
tracer = TracerFullReport(r'C:\Users\emman\OneDrive\Documentos\Verum\gambiarra\stagging')
df_tracer = tracer.get_report()
# print(df_tracer)
print(df_tracer.loc[df_tracer['tag'].str.contains('CF12-2780E', na=False)])