"""Pipeline defasado. Houve um produto no sudeste em que o intuito era receber o modelo IFC e verificá-lo para evidênciar erros. Atualmente este
teratamento de dados não está sendo utilizado e para reatiá-lo é necessário atualizar as fontes de dados e bibliotecas"""
from materials import Reports
from bim_tools import TracerFullReport
from suppliers import SuppliersLX
import os
import pandas as pd
import suppliers
import pipeline_tools

    
def newsteel():
    output_dir = os.environ['OUTPUT_ML_LX_NEWSTEEL']
    reports = reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_NEWSTEEL'])
    lx = suppliers.SuppliersLX(os.environ['LX_PATH_NEWSTEEL'], os.environ['MAPPER_PATH_NEWSTEEL'])
    lx._run_pipeline()
    df_tracer = tracer.get_report()
    df_desenho = reports.get_status_desenho()
    
    df_lx_error = lx.df_errors
    df_lx_report = lx.df_report

    df_numeric = df_lx_report[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx_report.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx_report = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])

    df_lx_report['cwp_number'] = df_lx_report['cwp'].str.split('-').str[2]
    
    df_lx_report['chave'] = df_lx_report['cwp_number'] + '-' + df_lx_report['tag'] 
    df_tracer['chave'] = df_tracer['cwa'] + '-' + df_tracer['marca'] 
    df_desenho['chave'] = df_desenho['cwp'] + '-' + df_desenho['tag'] 


    df_tracer = pd.merge(
        left=df_tracer,
        right=df_lx_report[['chave', 'qtd_lx']],
        on='chave',
        how='left'
    )
    df_tracer.loc[df_tracer['qtd_lx'].isna(), 'status'] = 'Não encontrado em LX'
    df_tracer.loc[~df_tracer['qtd_lx'].isna(), 'status'] = 'Encontrado em LX'
    df_tracer = pipeline_tools.breakdownbyaxis(df_tracer, axis='z', groupby='cwa_file')



    df_lx_report = pd.merge(
        left=df_lx_report,
        right=df_tracer[['chave', 'order']].sort_values(by='order', ascending=False).drop_duplicates(subset=['chave'], keep='first'),
        on='chave',
        how='left'
    )
    df_lx_report.loc[(df_lx_report['qtd_lx'] != df_lx_report['order']) & ~(df_lx_report['order'].isna()), 'status'] = 'Quantidade divergente LX vs ML'
    df_lx_report.loc[(df_lx_report['order'].isna()), 'status'] = 'Não existe no modelo'

    df_lx_error.to_parquet(os.path.join(output_dir, 'lx_error_reports.parquet'), index=False)
    df_lx_report.to_parquet(os.path.join(output_dir, 'lx.parquet'), index=False)
    df_tracer.to_parquet(os.path.join(output_dir, 'ifc_data.parquet'), index=False)
    df_desenho.to_parquet(os.path.join(output_dir, 'materials.parquet'), index=False)
    

