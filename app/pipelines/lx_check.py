from materials import Reports
from bim_tools import TracerFullReport
from suppliers import SuppliersLX
import os
import pandas as pd

def run_pipeline(reports_dir, tracer_data_dir, lx_dir, mapper_file, output_dir):
    reports = Reports(source_dir=reports_dir)
    tracer = TracerFullReport(source_dir=tracer_data_dir)
    lx = SuppliersLX(lx_dir, mapper_file)
    lx._run_pipeline()
    df_tracer = tracer.get_report()
    df_desenho = reports.get_status_desenho()
    

    df_lx_error = lx.df_errors
    df_lx_report = lx.df_report
    df_numeric = df_lx_report[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx_report.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx_report = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])
    
    df_lx_report['chave'] = df_lx_report['cwp'] + '-' + df_lx_report['tag'] 
    df_tracer['chave'] = df_tracer['cwa'] + '-' + df_tracer['marca'] 
    df_desenho['chave'] = df_desenho['cwp'] + '-' + df_desenho['tag'] 
    
    df_lx_error.to_parquet(os.path.join(output_dir, 'lx_error_reports.parquet'), index=False)
    df_lx_report.to_parquet(os.path.join(output_dir, 'lx.parquet'), index=False)
    df_tracer.to_parquet(os.path.join(output_dir, 'ifc_data.parquet'), index=False)
    df_desenho.to_parquet(os.path.join(output_dir, 'materials.parquet'), index=False)
    

    print(df_lx_error)
    print(df_lx_report['last_mod_date'])
    print(df_desenho)
    print(df_tracer.columns)


