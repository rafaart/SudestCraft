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
        df_recebimento[['tag', 'peso_un']],
        on='tag',
        how='left',
        suffixes=('_desenho', '_recebimento')
    )
    df_main.loc[df_main['peso_un_recebimento'].isna(), 'peso_un'] = df_main['peso_un_desenho']
    df_main.loc[df_main['peso_un'].isna(), 'peso_un'] = df_main['peso_un_recebimento']
    df_main['supplier'] = df_main['supplier'].fillna('Fornecedor não encontrado')
    df_main.to_parquet(os.path.join(output_dir, 'report_data.parquet'), index=False)
    df_main.to_csv(os.path.join(output_dir, 'report_data.csv'), index=False)



def capanema():
    output_dir=os.environ['OUTPUT_GESTAO_MATERIAIS_CAPANEMA']

    reports = Reports(source_dir=os.environ['REPORTS_PATH_CAPANEMA'])
    pq = suppliers.PQ(os.environ['MEMORIA_CALCULO_PATH_CAPANEMA'])
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
    df_main = pd.merge(
        df_main,
        df_recebimento[['tag', 'peso_un']],
        on='tag',
        how='left',
        suffixes=('_desenho', '_recebimento')
    )
    df_main.loc[df_main['peso_un_recebimento'].isna(), 'peso_un'] = df_main['peso_un_desenho']
    df_main.loc[df_main['peso_un'].isna(), 'peso_un'] = df_main['peso_un_recebimento']
    df_main['supplier'] = df_main['supplier'].fillna('Fornecedor não encontrado')
    df_main.to_parquet(os.path.join(output_dir, 'report_data.parquet'), index=False)
    df_main.to_csv(os.path.join(output_dir, 'report_data.csv'), index=False)