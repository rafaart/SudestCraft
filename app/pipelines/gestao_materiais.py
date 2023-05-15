import pandas as pd
import foundation
import suppliers
import os
from materials import Reports
from masterplan import Masterplan

def newsteel():
    output_dir = os.environ['OUTPUT_GESTAO_MATERIAIS_NEWSTEEL']

    construcap = suppliers.CronogramaMasterConstrucap(os.environ['MONTADORA_PATH_NEWSTEEL'])
    aumond = suppliers.Aumond(os.environ['FORNECEDORES_PATH_NEWSTEEL'])
    fam_mining = suppliers.FamMining(os.environ['FORNECEDORES_PATH_NEWSTEEL'])
    fam_structure = suppliers.FamStructure(os.environ['FORNECEDORES_PATH_NEWSTEEL'])
    pq = suppliers.PQSimplified(os.environ['PQ_PATH_NEWSTEEL'])
    suppliers_map = suppliers.SuppliersLX(os.environ['LX_PATH_NEWSTEEL'], os.environ['MAPPER_PATH_NEWSTEEL'])
    reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])

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
    df_main = pd.merge(
        df_main,
        df_recebimento[['tag', 'peso_un', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=('_desenho', '_recebimento')
    )
    df_main.loc[df_main['peso_un_recebimento'].isna(), 'peso_un'] = df_main['peso_un_desenho']
    df_main.loc[df_main['peso_un'].isna(), 'peso_un'] = df_main['peso_un_recebimento']
    df_main['supplier'] = df_main['supplier'].fillna(df_main['fornecedor'])
    df_main.to_parquet(os.path.join(output_dir, 'report_data.parquet'), index=False)



def capanema():
    output_dir=os.environ['OUTPUT_GESTAO_MATERIAIS_CAPANEMA']

    reports = Reports(source_dir=os.environ['REPORTS_PATH_CAPANEMA'])
    memoria_calculo = suppliers.MemoriaCalculo(os.environ['MEMORIA_CALCULO_PATH_CAPANEMA'])
    suppliers_map = suppliers.SuppliersLX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])

    flsmidth = suppliers.ModeloCronogramaCapanema('flsmidth', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    sinosteel_p1 = suppliers.ModeloCronogramaCapanema('sinosteel pct. 1', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    sinosteel_p2 = suppliers.ModeloCronogramaCapanema('sinosteel pct. 2', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    alfa = suppliers.ModeloCronogramaCapanema('alfa', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    codeme = suppliers.ModeloCronogramaCapanema('codeme', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    dpi = suppliers.ModeloCronogramaCapanema('dpi', os.environ['FORNECEDORES_PATH_CAPANEMA'])

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
    df_desenho = reports.get_status_desenho()
    df_recebimento = reports.get_recebimento()
    df_suppliers_map = suppliers_map.get_report()

    memoria_calculo._clean_report()
    df_memoria = memoria_calculo.report
    df_demontagem = memoria_calculo.cwp_desmontagem

    df_main = pd.merge(
        left=df_masterplan,
        right=df_suppliers,
        on='cwp',
        how='left',
        suffixes=('_masterplan', '_fornecedor')
    )
    df_main = pd.merge(
        left=df_main,
        right=df_memoria[['cwp', 'peso_capex_ton']].groupby('cwp', as_index=False).sum(),
        on='cwp',
        how='left',
        suffixes=(None, '_memoria_cwp')
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
    df_main = pd.merge(
        df_main,
        df_recebimento[['tag', 'peso_un', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=('_desenho', '_recebimento')
    )
    df_main.loc[df_main['peso_un_recebimento'].isna(), 'peso_un'] = df_main['peso_un_desenho']
    df_main.loc[df_main['peso_un'].isna(), 'peso_un'] = df_main['peso_un_recebimento']
    df_main['supplier'] = df_main['supplier'].fillna(df_main['fornecedor'])

    df_main = pd.merge(
        left=df_main,
        right=df_memoria[['cwp', 'fornecedor', 'peso_capex_ton']].rename(columns={'fornecedor':'supplier'}).groupby(['cwp','supplier'], as_index=False).sum(),
        on=['cwp','supplier'],
        how='outer',
        suffixes=(None, '_memoria_fornecedor')
    )
    df_main = df_main.loc[~df_main['cwp'].isin(df_demontagem['cwp'])]
    df_missing_supplier = df_main.loc[df_main['supplier'].isna() & ~df_main['peso_capex_ton'].isna(), ['cwp']].drop_duplicates(keep='first')
    df_missing_supplier['supplier_flag'] = False
    df_main = pd.merge(
        df_main,
        df_missing_supplier,
        on='cwp',
        how='left',
    )

    #############FILTERS
    df_main = df_main.loc[df_main['cwp'].str.contains('-S1985-', na=False) | df_main['cwp'].str.contains('CWP NÃO ENCONTRADO', na=False)]
    df_main = df_main.loc[~df_main['cwp'].str.contains('-M-SD-', na=False)]
    df_main = df_main.loc[~df_main['supplier'].isin(["00000004" ,"4100680511","4100690394","5500091398","90000959","90002705"])]
    #############FILTERS

    _get_view(df_main, os.path.join(output_dir, 'view_gestao_de_materiais.xlsx'))    
    df_main.to_parquet(os.path.join(output_dir, 'report_data.parquet'), index=False)
    df_memoria.to_parquet(os.path.join(output_dir, 'memoria_calculo.parquet'), index=False)
    


def _get_view(df, output_path):
    df_view = df[[
        'cwp',
        'tag',
        'supplier',
        'qtd_lx',
        'peso_un_lx',
        'qtd_desenho',
        'peso_un_desenho',
        'qtd_recebida',
        'peso_un',
        'descricao_desenho',
        'peso_capex_ton_memoria_fornecedor',
        'data_inicio_fornecedor',
        'data_termino_fornecedor',
        'data_inicio_masterplan',
    ]]
    df_view['data_inicio_fornecedor'] = df_view['data_inicio_fornecedor'].dt.date
    df_view['data_termino_fornecedor'] = df_view['data_termino_fornecedor'].dt.date
    df_view['data_inicio_masterplan'] = df_view['data_inicio_masterplan'].dt.date
    df_view.insert(13, 'prontidao', df_view['data_inicio_masterplan'] - pd.Timedelta(days=30))
    df_view.insert(3, 'peso_lx', df_view['qtd_lx'] * df_view['peso_un_lx'] / 1000)
    df_view.insert(4, 'peso_desenho', df_view['qtd_desenho'] * df_view['peso_un_desenho'] / 1000)
    df_view.insert(5, 'peso_recebido', df_view['qtd_recebida'] * df_view['peso_un'] / 1000)
    

    df_view_cwp = df_view[[
        'cwp',
        'supplier',
        'peso_lx',
        'peso_desenho',
        'peso_recebido',
        'peso_capex_ton_memoria_fornecedor',
        'data_inicio_fornecedor',
        'data_termino_fornecedor',
        'prontidao',
        'data_inicio_masterplan',
    ]]
    df_view_cwp = pd.merge(
        df_view_cwp.groupby(['cwp', 'supplier'], as_index=False).sum(),
        df_view_cwp[[
            'cwp', 
            'supplier', 
            'data_inicio_fornecedor', 
            'data_termino_fornecedor', 
            'prontidao', 
            'data_inicio_masterplan',
        ]].drop_duplicates(subset=['cwp', 'supplier']),
        on=['cwp', 'supplier']
    )
    df_view_cwp = df_view_cwp.rename(columns={
        'cwp': 'CWP',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'peso_desenho': 'Peso Materials (t)',
        'peso_recebido': 'Recebido (t)',
        'peso_capex_ton_memoria_fornecedor': 'Peso PQ (t)',
        'data_inicio_fornecedor': 'Start Tendência',
        'data_termino_fornecedor': 'Finish Tendência',
        'prontidao': 'Prontidão',
        'data_inicio_masterplan': 'Inicio de Montagem',
    })

    df_view_tag = df_view[[
        'cwp',
        'tag',
        'supplier',
        'peso_lx',
        'peso_desenho',
        'peso_recebido',
        'peso_un',
        'descricao_desenho',
    ]].dropna(subset=['tag'])
    
    df_view_tag = df_view_tag.rename(columns={
        'cwp': 'CWP',
        'tag': 'TAG',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'peso_desenho': 'Peso Materials (t)',
        'peso_recebido': 'Recebido (t)',
        'descricao_desenho': 'Descrição Materials',
    })

    writer = pd.ExcelWriter(output_path, engine = 'xlsxwriter')
    df_view_tag.to_excel(writer, sheet_name = 'Dados por Tag', index=False)
    df_view_cwp.to_excel(writer, sheet_name = 'Dados por CWP', index=False)
    writer.close()
