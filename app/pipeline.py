import pandas as pd
import foundation
import suppliers
import os
import pipeline_tools
from materials import Reports
from bim_tools import TracerFullReport
from masterplan import MasterplanDD, ListaMaster, Masterplan


def fornecimento_new_steel_fam(
        output_dir,
        reports_dir=os.environ['REPORTS_PATH_NEWSTEEL'],
        romaneio_dir=os.environ['ROMANEIO_PATH_NEWSTEEL'], 
        cronograma_dir=os.environ['MONTADORA_PATH_NEWSTEEL'], 
        tracer_data_dir=os.environ['TRACER_PATH_NEWSTEEL'], 
):
    cronograma_construcap = suppliers.CronogramaMasterConstrucap(cronograma_dir)
    romaneio = suppliers.RomaneioFAM(romaneio_dir)
    reports = Reports(source_dir=reports_dir)
    tracer = TracerFullReport(source_dir=tracer_data_dir)

    df_cronograma_construcap = cronograma_construcap.get_report()
    df_romaneio = romaneio.get_report()
    df_desenho = reports.get_status_desenho()
    df_recebimento = reports.get_recebimento()
    df_tracer = tracer.get_report()
    
    df = pd.merge(
        left=df_romaneio,
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
    df = pipeline_tools.get_quantities_fam(df, df_recebimento)

    df_tracer = pd.merge(
        left=df_tracer, 
        right=df, 
        how='left', 
        on='chave',
        suffixes=[None, '_romaneio']
    )   
    df_tracer.loc[df_tracer['qtd_total'].isna(), 'chave'] = 'CWA' + df_tracer['cwa']

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_fam, axis=1)
    df_tracer = pipeline_tools.break_down_ifc_fam(df_tracer)

    df_tracer.to_parquet(os.path.join(output_dir, 'tracer_data.parquet'), index=False)
    df.to_parquet(os.path.join(output_dir, 'inventory_data.parquet'), index=False)



def fornecimento_capanema_codeme(
        output_dir,
        reports_dir=os.environ['REPORTS_PATH_CAPANEMA'], 
        lx_dir=os.environ['LX_PATH_CAPANEMA'], 
        mapper_file=os.environ['MAPPER_PATH_CAPANEMA'], 
        cronogramas_masterplan_dir=os.environ['MASTERPLAN_PATH_CAPANEMA'], 
        tracer_data_dir=os.environ['TRACER_PATH_CAPANEMA'], 
):
    masterplan = Masterplan(cronogramas_masterplan_dir)
    lx = suppliers.SuppliersLX(lx_dir, mapper_file)
    reports = Reports(source_dir=reports_dir)
    tracer = TracerFullReport(source_dir=tracer_data_dir)

    df_lx = lx.get_report()
    df_numeric = df_lx[['cwp', 'cod_ativo', 'tag', 'qtd_lx']].groupby(['cwp', 'cod_ativo', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'cod_ativo', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'cod_ativo', 'tag'])
    df_lx = df_lx.loc[df_lx['supplier'].str.contains('CODEME', na=False)]

    df_main = pd.merge(
        left=df_lx,
        right=reports.get_status_desenho(), 
        on=['chave'],
        how='left',
        suffixes=(None, '_materials')
    )

    df_main = pd.merge(
        left=df_main,
        right=masterplan.get_report(), 
        on='cwp',
        how='left',
        suffixes=(None, '_masterplan')
    )
    
    df_main = pipeline_tools.get_quantities(df_main.sort_values(by='data_inicio', ascending=True), reports.get_recebimento())
    df_main['qtd_faltante'] = df_main['qtd_lx'] - df_main['qtd_recebida']
    df_fill = df_main[['cwp', 'cwp_number', 'sheet_name', 'cod_ativo']].drop_duplicates(subset=['cwp_number'], keep='first')
    df_fill['tag'] = df_main['cwp_number'].astype(str) + df_main['cod_ativo'].str.replace('ED', '')
    df_fill = df_fill.drop_duplicates(subset=['cwp_number'], keep='first')
    df_main = pd.concat([df_main,df_fill], ignore_index=True)
    df_main['chave'] = df_main['cwp_number'].str.zfill(3) + '-' + df_main['tag']
    df_main['cod_navegacao'] = df_main.apply(lambda row: row['cwp_number'] + row['cod_ativo'].replace('ED', ''), axis=1)
    
    df_tracer = tracer.get_report()
    df_tracer = df_tracer.loc[df_tracer['file_name'].str.split('-').str[0].isin(df_main['cwp_number'])]
    df_main = df_main.loc[df_main['cwp_number'].isin(df_tracer['file_name'].str.split('-').str[0])]

    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_main[['tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_faltante', 'data_inicio', 'peso_un']],
        on='tag',
        how='left'
    )   

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_codeme, axis=1)
    df_tracer.loc[~df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), 'chave'] = df_tracer['cwp'] + "-" + df_tracer['pwp']
    df_tracer.loc[df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), 'chave'] = df_tracer['cwp'] + "-" +df_tracer['tag']
    df_tracer = pipeline_tools.break_down_ifc_codeme(df_tracer)

    df_tracer.to_parquet(os.path.join(output_dir, 'tracer_data.parquet'), index=False)
    df_main.to_parquet(os.path.join(output_dir, 'inventory_data.parquet'), index=False)


def fornecimento_capanema_sinosteel(
        output_dir,
        reports_dir=os.environ['REPORTS_PATH_CAPANEMA'], 
        lx_dir=os.environ['LX_PATH_CAPANEMA'], 
        mapper_file=os.environ['MAPPER_PATH_CAPANEMA'], 
        cronogramas_masterplan_dir=os.environ['MASTERPLAN_PATH_CAPANEMA'], 
        tracer_data_dir=os.environ['TRACER_PATH_CAPANEMA'], 
):

    masterplan = Masterplan(cronogramas_masterplan_dir)
    lx = suppliers.SuppliersLX(lx_dir, mapper_file)
    reports = Reports(source_dir=reports_dir)
    tracer = TracerFullReport(source_dir=tracer_data_dir)
    df_lx = lx.get_report()

    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])
    df_lx = df_lx.loc[df_lx['supplier'].str.contains('SINOSTEEL', na=False)]

    df_main = pd.merge(
        left=df_lx,
        right=reports.get_status_desenho(), 
        on=['chave'],
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
    df_fill = df_main[['cwp', 'cwp_number']]
    df_fill['tag'] = df_main['cwp_number']
    df_fill = df_fill.drop_duplicates(subset=['cwp_number'], keep='first')
    df_main = pd.concat([df_main,df_fill], ignore_index=True)
    df_main['chave'] = df_main['cwp_number'].str.zfill(3) + '-' + df_main['tag']
    df_main['cod_navegacao'] = 'CWP' + df_main['cwp_number']
   
    df_tracer = tracer.get_report()
    df_tracer = df_tracer.loc[df_tracer['file_name'].str.split('-').str[0].isin(df_main['cwp_number'])]
    df_main = df_main.loc[df_main['cwp_number'].isin(df_tracer['file_name'].str.split('-').str[0])]

    df_tracer = pd.merge(
        left=tracer.get_report(), 
        right=df_main[['tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_faltante', 'data_inicio', 'peso_un']],
        on='tag',
        how='left'
    )   

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_sinosteel, axis=1)
    df_tracer.loc[~df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), 'chave'] = df_tracer['cwp'] + "-" + df_tracer['pwp']
    df_tracer.loc[df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), 'chave'] = df_tracer['cwp'] + "-" +df_tracer['tag']
    df_tracer = pipeline_tools.break_down_ifc_sinosteel(df_tracer)

    df_tracer.to_parquet(os.path.join(output_dir, 'tracer_data.parquet'), index=False)
    df_main.to_parquet(os.path.join(output_dir, 'inventory_data.parquet'), index=False)


def gestao_materiais_newsteel(
        output_dir,
        reports_dir=os.environ['REPORTS_PATH_NEWSTEEL'],
        cronogramas_fornecedores_dir=os.environ['FORNECEDORES_PATH_NEWSTEEL'], 
        cronogramas_montadora_dir=os.environ['MONTADORA_PATH_NEWSTEEL'], 
        pq_dir=os.environ['PQ_PATH_NEWSTEEL'], 
        lx_dir=os.environ['LX_PATH_NEWSTEEL'], 
        mapper_file=os.environ['MAPPER_PATH_NEWSTEEL'],
):
    construcap = suppliers.CronogramaMasterConstrucap(cronogramas_montadora_dir)
    aumond = suppliers.Aumond(cronogramas_fornecedores_dir)
    fam_mining = suppliers.FamMining(cronogramas_fornecedores_dir)
    fam_structure = suppliers.FamStructure(cronogramas_fornecedores_dir)
    pq = suppliers.PQSimplified(pq_dir)
    suppliers_map = suppliers.SuppliersLX(lx_dir, mapper_file)
    reports = Reports(source_dir=reports_dir)

    df_construcap = construcap.get_report()
    df_aumond = aumond.get_report()
    df_fam_mining = fam_mining.get_report()
    df_fam_structure = fam_structure.get_report()
    df_pq = pq.get_report()
    df_desenho = reports.get_status_desenho()
    df_recebimento = reports.get_recebimento()
    df_suppliers_map = suppliers_map.get_report()

    df_suppliers = pd.concat([df_aumond, df_fam_mining, df_fam_structure])
    df_suppliers = df_suppliers.sort_values(by='data_termino', ascending=True).drop_duplicates(subset='cwp' ,keep='last')

    df_main = pd.merge(
        left=df_construcap,
        right=df_suppliers,
        on='cwp',
        how='left',
        suffixes=('_construcap', '_fornecedor')
    )

    df_main = pd.merge(
        left=df_main,
        right=df_pq,
        on='cwp',
        how='left',
        suffixes=('_cronograma', '_pq')
    )

    df_main = pd.merge(
        left=df_main,
        right=df_desenho,
        on='cwp',
        how='outer',
        suffixes=('_cronograma', '_desenho')
    )
    df_main = pd.merge(
        left=df_main,
        right=df_suppliers_map,
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_lx')
    )

    df_main = Reports._get_quantities(df_main, df_recebimento)
    df_main = df_main.drop(columns=['obs'])
    df_main.to_parquet(os.path.join(output_dir, 'report_data.parquet'), index=False)



def gestao_materiais_capanema(
        output_dir,
        reports_dir=os.environ['REPORTS_PATH_CAPANEMA'], 
        lx_dir=os.environ['LX_PATH_CAPANEMA'], 
        mapper_file=os.environ['MAPPER_PATH_CAPANEMA'], 
        cronogramas_masterplan_dir=os.environ['MASTERPLAN_PATH_CAPANEMA'],
        pq_dir=os.environ['PQ_PATH_CAPANEMA'], 
        cronogramas_fornecedores_dir=os.environ['FORNECEDORES_PATH_CAPANEMA'], 
):
    reports = Reports(source_dir=reports_dir)
    pq = suppliers.PQ(pq_dir)
    suppliers_map = suppliers.SuppliersLX(lx_dir, mapper_file)
    masterplan = Masterplan(cronogramas_masterplan_dir)

    flsmidth = suppliers.ModeloCronogramaCapanema('flsmidth', cronogramas_fornecedores_dir)
    sinosteel_p1 = suppliers.ModeloCronogramaCapanema('sinosteel pct. 1', cronogramas_fornecedores_dir)
    sinosteel_p2 = suppliers.ModeloCronogramaCapanema('sinosteel pct. 2', cronogramas_fornecedores_dir)
    alfa = suppliers.ModeloCronogramaCapanema('alfa', cronogramas_fornecedores_dir)
    codeme = suppliers.ModeloCronogramaCapanema('codeme', cronogramas_fornecedores_dir)
    dpi = suppliers.ModeloCronogramaCapanema('dpi', cronogramas_fornecedores_dir)

    df_suppliers = pd.concat([
        flsmidth.get_report(), 
        sinosteel_p1.get_report(),
        sinosteel_p2.get_report(),
        alfa.get_report(),
        codeme.get_report(),
        dpi.get_report()
    ])
    df_suppliers = df_suppliers.sort_values(by='data_termino', ascending=True).drop_duplicates(subset='cwp' ,keep='last')

    df_masterplan = masterplan.get_report()
    df_pq = pq.get_report()
    df_desenho = reports.get_status_desenho()
    df_recebimento = reports.get_recebimento()
    df_suppliers_map = suppliers_map.get_report()

    df_main = pd.merge(
        left=df_masterplan,
        right=df_suppliers,
        on='cwp',
        how='left',
        suffixes=('_masterplan', '_fornecedor')
    )
    df_main = pd.merge(
        left=df_main,
        right=df_pq,
        on='cwp',
        how='left',
        suffixes=(None, '_pq')
    )
    df_main = pd.merge(
        left=df_main,
        right=df_desenho,
        on='cwp',
        how='outer',
        suffixes=(None, '_desenho')
    )
    df_main = pd.merge(
        left=df_main,
        right=df_suppliers_map,
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_lx')
    )
    df_main = Reports._get_quantities(df_main.sort_values(by='data_inicio_masterplan', ascending=True), df_recebimento)
    df_main.to_parquet(os.path.join(output_dir, 'report_data.parquet'), index=False)


