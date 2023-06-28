import os
import pandas as pd
from pipelines import pipeline_tools
from data_sources.suppliers import CronogramaMasterConstrucap
from data_sources.LX import LX
from data_sources.foundation import Summary
from data_sources.materials import Reports
from data_sources.masterplan import ListaMaster, Masterplan


def newsteel():
    output_dir = os.environ['OUTPUT_MONTAGEM_ELETROMECANICA_NEWSTEEL']
    summary = Summary(os.environ['SUMMARY_PATH_NEWSTEEL'])
    masterplan = CronogramaMasterConstrucap(os.environ['MONTADORA_PATH_NEWSTEEL'])
    reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])
    reports.clean_reports()

    df_summary = summary.get_report()
    df_masterplan = masterplan.get_report()
    df_recebimento = reports.df_recebimento
    df_desenho = reports.df_desenho
    df_distribuicao = reports.df_distribuicao

    df_iwp = pd.merge(
        left=df_summary,
        right=df_desenho,
        on=['cwp', 'tag'],
        how='left',
        suffixes=['_summary', '_desenho']
    ).drop(columns='qtd_desenho').rename(columns={'qtd_summary': 'qtd_desenho'})

    df_iwp = df_iwp.sort_values(by=['data_inicio', 'iwp'], ascending=True)
    df_iwp = pipeline_tools.get_quantities_montagem_eletromecanica(df_iwp, df_distribuicao.copy().groupby(by=['iwp', 'tag'], as_index=False).sum(numeric_only=True), by=['iwp', 'tag'])
    df_unknow_iwp = df_distribuicao.loc[~df_distribuicao[['iwp', 'tag']].apply(tuple,1).isin(df_iwp[['iwp', 'tag']].apply(tuple,1)), ['tag', 'qtd_solicitada', 'qtd_entregue']]
    df_unknow_iwp = df_unknow_iwp.groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_iwp = pipeline_tools._predict_stock(df_iwp, df_unknow_iwp)
    df_iwp = Reports._get_quantities(df_iwp.sort_values(by='data_inicio', ascending=True), df_recebimento.copy())
    df_iwp.to_parquet(os.path.join(output_dir, 'iwp_data.parquet'), index=False)
    
    df_cwp = pd.merge(
        left=df_desenho,
        right=df_summary[['cwp', 'data_inicio']].sort_values(by='data_inicio', ascending=True).drop_duplicates(subset=['cwp'], keep='first'),
        on=['cwp'],
        how='left',
        suffixes=('_desenho', '_summary')
    )
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_masterplan[['cwp', 'descricao', 'data_inicio']],
        on='cwp',
        how='left',
        suffixes=(None, '_masterplan')
    )
    df_cwp.loc[df_cwp['data_inicio'].isna(), 'data_inicio'] = df_cwp['data_inicio_masterplan']
    df_cwp = df_cwp.sort_values(by='data_inicio', ascending=True, na_position='last')
    df_cwp = pipeline_tools.get_quantities_montagem_eletromecanica(df_cwp, df_distribuicao.copy().groupby(by=['cwp', 'tag'], as_index=False).sum(numeric_only=True), by=['cwp','tag'])
    df_unknow_cwp = df_distribuicao.loc[~df_distribuicao[['cwp', 'tag']].apply(tuple,1).isin(df_cwp[['cwp', 'tag']].apply(tuple,1)), ['cwp', 'tag', 'qtd_solicitada', 'qtd_entregue']]
    df_unknow_cwp = df_unknow_cwp.groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_cwp = pipeline_tools._predict_stock(df_cwp, df_unknow_cwp)
    df_cwp = Reports._get_quantities(df_cwp.sort_values(by='data_inicio', ascending=True), df_recebimento)
    
    df_cwp.to_parquet(os.path.join(output_dir, 'cwp_data.parquet'), index=False)


def capanema():
    output_dir = os.environ['OUTPUT_MONTAGEM_ELETROMECANICA_CAPANEMA']
    summary = Summary(os.environ['SUMMARY_PATH_CAPANEMA'])
    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lista_master = ListaMaster(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = LX(os.environ['LX_PATH_CAPANEMA'])
    lx_sinosteel = LX(r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\SMAT\LX\SINOSTEEL\LX_GERAL_SINOSTEEL')
    reports = Reports(source_dir=os.environ['REPORTS_PATH_CAPANEMA'])
    reports.clean_reports()

    df_summary = summary.get_report()
    df_masterplan = masterplan.get_report()
    df_lista_master = lista_master.get_report()
    df_recebimento = reports.df_recebimento
    df_desenho = reports.df_desenho
    df_distribuicao = reports.df_distribuicao

    lx_sinosteel.config['depth'] = 0
    lx_sinosteel._run_pipeline()
    df_lx_sinosteel = lx_sinosteel.df_lx   
    df_lx_sinosteel['supplier'] = 'SINOSTEEL'

    df_lx = lx.get_report()
    df_lx = df_lx.loc[df_lx['supplier'] != 'SINOSTEEL']
    df_lx = pd.concat([df_lx, df_lx_sinosteel])

    df_iwp = pd.merge(
        left=df_summary,
        right=df_desenho,
        on=['cwp', 'tag'],
        how='left',
        suffixes=['_summary', '_desenho']
    ).drop(columns='qtd_desenho').rename(columns={'qtd_summary': 'qtd_desenho'})
    df_distribuicao_no_iwp = df_distribuicao.loc[df_distribuicao['iwp'].isna()]
    df_distribuicao_iwp = df_distribuicao.loc[~df_distribuicao['iwp'].isna()]
    df_distribuicao_iwp = df_distribuicao_iwp[['iwp', 'tag', 'qtd_solicitada', 'qtd_entregue']].groupby(['iwp', 'tag'], as_index=False).sum()
    
    df_iwp = pd.merge(
        left=df_iwp,
        right=df_distribuicao_iwp,
        on=['iwp', 'tag'],
        how='outer',
        suffixes=[None, '_distribuicao']
    )
    df_iwp = df_iwp.sort_values(by=['data_inicio', 'iwp'], ascending=True)
    df_distribuicao_no_iwp = df_distribuicao_no_iwp[['tag', 'qtd_solicitada', 'qtd_entregue']].groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_iwp = pipeline_tools._predict_stock(df_iwp, df_distribuicao_no_iwp)
    df_iwp = Reports._get_quantities(df_iwp.sort_values(by='data_inicio', ascending=True), df_recebimento.copy())
    df_iwp = pd.merge(
        df_iwp,
        df_recebimento[['tag', 'peso_un_recebimento', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=(None, '_recebimento')
    )
    
    df_iwp = pd.merge(
        left=df_iwp,
        right=df_lx,
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_lx')
    )
    df_iwp['supplier'] = df_iwp['supplier'].fillna(df_iwp['fornecedor'])
    df_iwp.loc[df_iwp['peso_un_recebimento'].isna(), 'peso_un'] = df_iwp['peso_un_desenho']
    df_iwp.loc[df_iwp['peso_un'].isna(), 'peso_un'] = df_iwp['peso_un_recebimento']
    df_iwp.to_parquet(os.path.join(output_dir, 'iwp_data.parquet'), index=False)
    
    df_cwp = pd.merge(
        left=df_desenho,
        right=df_summary[['cwp', 'data_inicio']].sort_values(by='data_inicio', ascending=True).drop_duplicates(subset=['cwp'], keep='first'),
        on=['cwp'],
        how='left',
        suffixes=(None, '_summary')
    )    
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_masterplan[['cwp', 'data_inicio']],
        on='cwp',
        how='left',
        suffixes=(None, '_masterplan')
    )    
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_lista_master[['cwp', 'descricao_cwp']],
        on='cwp',
        how='left',
        suffixes=(None, '_lista_master')
    )
    df_cwp.loc[df_cwp['data_inicio'].isna(), 'data_inicio'] = df_cwp['data_inicio_masterplan']
    df_cwp = df_cwp.sort_values(by='data_inicio', ascending=True, na_position='last')
    df_cwp = pipeline_tools.get_quantities_montagem_eletromecanica(df_cwp, df_distribuicao.groupby(by=['cwp', 'tag'], as_index=False).sum(numeric_only=True), by=['cwp','tag'])
    df_unknow_cwp = df_distribuicao.loc[~df_distribuicao[['cwp', 'tag']].apply(tuple,1).isin(df_cwp[['cwp', 'tag']].apply(tuple,1)), ['cwp', 'tag', 'qtd_solicitada', 'qtd_entregue']]
    df_unknow_cwp = df_unknow_cwp.groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_cwp = pipeline_tools._predict_stock(df_cwp, df_unknow_cwp)
    df_cwp = Reports._get_quantities(df_cwp.sort_values(by='data_inicio', ascending=True), df_recebimento)
    df_cwp = pd.merge(
        df_cwp,
        df_recebimento[['tag', 'peso_un_recebimento', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=(None, '_recebimento')
    )
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_lx,
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_lx')
    )
    df_cwp['supplier'] = df_cwp['supplier'].fillna(df_cwp['fornecedor'])
    df_cwp.loc[df_cwp['peso_un_desenho'].isna(), 'peso_un_desenho'] = df_cwp['peso_un_recebimento']

    df_cwp.to_parquet(os.path.join(output_dir, 'cwp_data.parquet'), index=False)