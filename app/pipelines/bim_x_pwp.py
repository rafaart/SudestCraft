import pandas as pd
import suppliers
import os
import pipeline_tools
from materials import Reports
from bim_tools import TracerFullReport
from masterplan import Masterplan

pd.options.mode.chained_assignment = None

def famsteel():
    output_dir = os.environ['OUTPUT_FAM_NEWSTEEL']

    cronograma_construcap = suppliers.CronogramaMasterConstrucap(os.environ['MONTADORA_PATH_NEWSTEEL'])
    producao = suppliers.ProducaoFAM(os.environ['PRODUCAO_PATH_NEWSTEEL'])
    reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_NEWSTEEL'])

    df_cronograma_construcap = cronograma_construcap.get_report()
    df_producao = producao.get_report()
    df_desenho = reports.get_status_desenho()
    df_recebimento = reports.get_recebimento()

    df_producao['chave'] = df_producao['chave'].replace(' ', '')

    df_desenho['cwa'] = df_desenho['cwp'].str.split('-').str[2].str.zfill(3)
    df_desenho['tag'] = df_desenho['tag'].replace(' ', '')
    df_desenho['chave'] = 'CWA' + df_desenho['cwa'] + '-' + df_desenho['tag']
    

    df = pd.merge(
        left=df_producao,
        right=df_desenho, 
        on='chave',
        how='left',
        suffixes=(None, '_desenho')
    )

    df = pd.merge(
        left=df,
        right=df_cronograma_construcap[['data_inicio', 'cwp']],
        on='cwp',        
        how='left'
    )

    df_fill  = df[['cwa', 'chave']].drop_duplicates(subset=['cwa'], keep='first')
    df_fill['chave'] = df_fill['cwa']

    df = pd.concat([df, df_fill], ignore_index=True)
    df_recebimento['tag'] = df_recebimento['tag'].replace(' ', '')
    df = pipeline_tools.get_quantities_fam(df, df_recebimento)

    tracer.read_stagging_data()
    tracer.df_raw_report = tracer.df_raw_report.loc[~tracer.df_raw_report['file_name'].str.contains('VG-P0400', na=False)]
    df_tracer = tracer.get_report()
    df_tracer['tag'] = df_tracer['tag'].str.replace(' ', '')
    df_tracer['chave'] = 'CWA' + df_tracer['file_name'] + '-' + df_tracer['tag']
    df_tracer = pd.merge(
        left=df_tracer, 
        right=df, 
        how='left', 
        on='chave',
        suffixes=[None, '_romaneio']
    )
    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_fam, axis=1)
    df_tracer.to_parquet(os.path.join(output_dir, 'tracer_data.parquet'), index=False)
    df.to_parquet(os.path.join(output_dir, 'inventory_data.parquet'), index=False)



def emalto():
    output_dir = os.environ['OUTPUT_EMALTO_NEWSTEEL']

    cronograma_construcap = suppliers.CronogramaMasterConstrucap(os.environ['MONTADORA_PATH_NEWSTEEL'])
    producao = suppliers.ProducaoEMALTO(os.environ['PRODUCAO_PATH_NEWSTEEL'])
    romaneio = suppliers.RomaneioEMALTO(os.environ['ROMANEIO_PATH_NEWSTEEL'])
    reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_NEWSTEEL'])

    df_cronograma_construcap = cronograma_construcap.get_report()
    df_producao = producao.get_report()
    df_desenho = reports.get_status_desenho()
    df_recebimento = reports.get_recebimento()
    df_tracer = tracer.read_stagging_data().get_report()
    df_romaneio = romaneio.get_report()

    df_desenho['cwa_number'] = df_desenho['cwp'].str.split('-').str[2]
    df = pd.merge(
        left=df_producao,
        right=df_desenho, 
        on=['cwa_number', 'tag'],
        how='left',
        suffixes=(None, '_desenho')
    )
    df = pd.merge(
        left=df,
        right=df_cronograma_construcap[['data_inicio', 'cwp']],
        on='cwp',        
        how='left'
    )
    df_romaneio_only = df_romaneio.loc[~df_romaneio['tag'].isin(df['tag'])].rename(columns={'qtd_romaneio' :'qtd_total'})
    df_romaneio_only['cwa_number'] = df_romaneio_only['cwa'].str.extract('(\d+)')
    df = pd.concat([
        df,
        df_romaneio_only,
    ], axis=0)

    df, df_romaneio = pipeline_tools.consume_warehouse(df, 'qtd_total', df_romaneio, 'qtd_romaneio')
    df, df_recebimento = pipeline_tools.consume_warehouse(df, 'qtd_total', df_recebimento, 'qtd_recebida')
        
    df['peso_romaneio'] = df['qtd_romaneio'] * df['peso_un'] 
    df['peso_recebido'] = df['qtd_recebida'] * df['peso_un'] 

    qtd_columns = ['qtd_programacao', 'qtd_preparacao', 'qtd_fabricacao', 'qtd_expedicao']
    df[qtd_columns] = df[qtd_columns].subtract(df['qtd_romaneio'], axis=0)
    qtd_columns += ['qtd_romaneio']
    df[qtd_columns] = df[qtd_columns].subtract(df['qtd_recebida'], axis=0)
    df[qtd_columns] = df[qtd_columns].applymap(lambda x: 0 if x <0 else x)

    peso_columns = ['peso_programacao', 'peso_preparacao', 'peso_fabricacao', 'peso_expedicao']
    df[peso_columns] = df[peso_columns].subtract(df['peso_romaneio'], axis=0)
    peso_columns += ['peso_romaneio']
    df[peso_columns] = df[peso_columns].subtract(df['peso_recebido'], axis=0)
    df[peso_columns] = df[peso_columns].applymap(lambda x: 0 if x <0 else round(x, 3))

    df_fill  = df[['cwa_number']].drop_duplicates(keep='first')
    df = pd.concat([df, df_fill], ignore_index=True)
    df['chave'] = df['cwa_number'] + '-' + df['tag'].fillna('')

    df_tracer = df_tracer.loc[df_tracer['file_name'].str.contains('VG-P0400', na=False)]
    df_tracer['cwa_number'] = df_tracer['file_name'].str.split('-').str[2]
    df_tracer['chave'] = df_tracer['cwa_number'] + '-' + df_tracer['tag'].fillna('')

    df_tracer = pd.merge(
        left=df_tracer, 
        right=df, 
        how='left', 
        on=['cwa_number', 'tag'],
        suffixes=[None, '_romaneio']
    )   
    df_tracer[qtd_columns] = df_tracer[qtd_columns ].fillna(0)
    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_emalto, axis=1)
    df_tracer.loc[df_tracer['status'] == '7.Inconsistente', 'chave'] = df_tracer['cwa_number'] + '-'
    df_tracer.to_csv(os.path.join(output_dir, 'tracer_data.csv'), index=False)
    df.to_csv(os.path.join(output_dir, 'inventory_data.csv'), index=False)



def codeme():
    output_dir = os.environ['OUTPUT_CODEME_CAPANEMA'] 

    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = suppliers.LX(os.environ['LX_PATH_CAPANEMA'])
    reports = Reports(os.environ['REPORTS_PATH_CAPANEMA'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_CAPANEMA'])

    lx._run_pipeline()
    print(lx.df_errors)
    df_lx = lx.df_lx
    df_lx['tag'] = df_lx['tag'].str.replace('1220CF01', '1220CF-01')

    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])
    df_lx = df_lx.loc[df_lx['supplier'].str.contains('CODEME', na=False)]

    df_main = pd.merge(
        left=df_lx,
        right=reports.get_status_desenho(), 
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_materials')
    )
    
    df_main = pd.merge(
        left=df_main,
        right=masterplan.get_report(), 
        on='cwp',
        how='left'
    )
    
    df_main = pipeline_tools.get_quantities(df_main.sort_values(by='data_inicio', ascending=True), reports.get_recebimento())
    df_main['qtd_faltante'] = df_main['qtd_lx'] - df_main['qtd_recebida']
    df_main['cod_navegacao'] = df_main['cwp_number'] + '-' + df_main['cod_ativo']
    df_main['chave'] = df_main['cwp'] + '-' + df_main['tag']

    df_fill = df_main[['cwp', 'cod_navegacao']].drop_duplicates(subset=['cwp'], keep='first')
    df_fill['chave'] = df_fill['cwp']
    df_main = pd.concat([df_main,df_fill], ignore_index=True)
    
    df_tracer = tracer.read_stagging_data().drop_missplaced_elements().get_report()
    df_tracer = df_tracer.loc[df_tracer['cwp'].isin(df_main['cwp'].drop_duplicates(keep='first'))]
    df_main = df_main.loc[df_main['cwp'].isin(df_tracer['cwp'].drop_duplicates(keep='first'))]

    df_tracer['cod_ativo'] = df_tracer['file_name'].str[26:]
    df_tracer = df_tracer.loc[df_tracer['cwp'] == df_tracer['file_name'].str[0:25]]
    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_main[['cwp', 'tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_faltante', 'data_inicio', 'peso_un']],
        on=['cwp', 'tag'],
        how='left'
    )  

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_codeme, axis=1)
    df_tracer.loc[~df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), ['chave', ]] = df_tracer['cwp']
    df_tracer.loc[df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), 'chave'] = df_tracer['cwp'] + "-" +df_tracer['tag']
    df_tracer['part'] = None
    df_ativos = df_tracer['cod_ativo'].drop_duplicates(keep='first')
    for cod in df_ativos:
        sample_size = len(df_tracer.loc[df_tracer['cod_ativo'] == cod])
        if 11000 >= sample_size:
            df_tracer.loc[df_tracer['cod_ativo'] == cod] = pipeline_tools.breakdown_by_axis(df_tracer.loc[df_tracer['cod_ativo'] == cod], 'file_name', 'location_z', 1)
        if 20000 >= sample_size > 11000:
            df_tracer.loc[df_tracer['cod_ativo'] == cod] = pipeline_tools.breakdown_by_axis(df_tracer.loc[df_tracer['cod_ativo'] == cod], 'file_name', 'location_z', 2)
        if 30000 >=  sample_size > 20000:
            df_tracer.loc[df_tracer['cod_ativo'] == cod] = pipeline_tools.breakdown_by_axis(df_tracer.loc[df_tracer['cod_ativo'] == cod], 'file_name', 'location_z', 4)
        if sample_size > 30000:
            df_tracer.loc[df_tracer['cod_ativo'] == cod] = pipeline_tools.breakdown_by_axis(df_tracer.loc[df_tracer['cod_ativo'] == cod], 'file_name', 'location_z', 10)

    df_tracer.to_parquet(os.path.join(output_dir, 'tracer_data.parquet'), index=False)
    df_main.to_parquet(os.path.join(output_dir, 'inventory_data.parquet'), index=False)



def sinosteel():
    output_dir = os.environ['OUTPUT_SINOSTEEL_CAPANEMA'] 

    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = suppliers.LX(os.environ['LX_PATH_CAPANEMA'])
    reports = Reports(os.environ['REPORTS_PATH_CAPANEMA'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_CAPANEMA'])
    df_lx = lx.get_report()

    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])
    df_lx = df_lx.loc[df_lx['supplier'].str.contains('SINOSTEEL', na=False)]

    df_main = pd.merge(
        left=df_lx,
        right=reports.get_status_desenho(), 
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_materials')
    )

    df_main = pd.merge(
        left=df_main,
        right=masterplan.get_report(), 
        on='cwp',
        how='left'
    )

    df_main = pipeline_tools.get_quantities(df_main.sort_values(by='data_inicio', ascending=True), reports.get_recebimento())
    df_main['qtd_faltante'] = df_main['qtd_lx'] - df_main['qtd_recebida']
    df_main['cod_navegacao'] = 'CWP' + df_main['cwp_number']
    df_main['chave'] = df_main['cwp'] + '-' + df_main['tag']

    df_fill = df_main[['cwp', 'cod_navegacao']].drop_duplicates(subset=['cwp'], keep='first')
    df_fill['chave'] = df_fill['cwp']
    df_main = pd.concat([df_main,df_fill], ignore_index=True)
    
    df_tracer = tracer.read_stagging_data().drop_missplaced_elements().get_report()
    df_tracer = df_tracer.loc[df_tracer['cwp'].isin(df_main['cwp'].drop_duplicates(keep='first'))]

    df_tracer = df_tracer.loc[df_tracer['cwp'] == df_tracer['file_name'].str[0:25]]
    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_main[['cwp', 'tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_faltante', 'data_inicio', 'peso_un']],
        on=['cwp', 'tag'],
        how='left'
    )  

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_sinosteel, axis=1)
    df_tracer.loc[~df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), 'chave'] = df_tracer['cwp']
    df_tracer.loc[df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), 'chave'] = df_tracer['cwp'] + "-" +df_tracer['tag']
    df_tracer = pipeline_tools.breakdown_by_axis(df_tracer, 'file_name', 'location_x', 2)
    for file_name in df_tracer['file_name'].drop_duplicates(keep='first'):
        if len(df_tracer.loc[df_tracer['file_name'] == file_name]) > 30000:
            df_tracer.loc[df_tracer['file_name'] == file_name] = pipeline_tools.breakdown_by_axis(df_tracer.loc[df_tracer['file_name'] == file_name], 'file_name', 'location_x', 9)
    df_tracer = pipeline_tools.breakdown_by_file_count(df_tracer, 'cwp', 2)

    df_tracer.to_parquet(os.path.join(output_dir, 'tracer_data.parquet'), index=False)
    df_main.to_parquet(os.path.join(output_dir, 'inventory_data.parquet'), index=False)