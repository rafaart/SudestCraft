import pandas as pd
import os
from pipelines import pipeline_tools
from data_sources.materials import Reports
from data_sources.ifc_sources import TracerFullReport, Vcad
from data_sources.masterplan import Masterplan
from data_sources.suppliers import CronogramaMasterConstrucap
from data_sources.LX import LX
import ifcopenshell
import ifcopenshell.api

pd.options.mode.chained_assignment = None


def newsteel():
    output_dir = os.environ['FEDERATED_PATH_NEWSTEEL']
    ifc_dir = os.environ['IFC_PATH_NEWSTEEL']

    cronograma_construcap = CronogramaMasterConstrucap(os.environ['MONTADORA_PATH_NEWSTEEL'])
    lx = LX(os.environ['LX_PATH_NEWSTEEL'])
    reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_NEWSTEEL'])
    df_lx = lx.get_report()

    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])

    df_main = pd.merge(
        left=df_lx,
        right=reports.get_status_desenho(), 
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_materials')
    )

    df_main = pd.merge(
        left=df_main,
        right=cronograma_construcap.get_report(), 
        on='cwp',
        how='left'
    )
    df_main = pipeline_tools.get_quantities(df_main.sort_values(by='data_inicio', ascending=True), reports.get_recebimento())
    df_main['qtd_faltante'] = df_main['qtd_lx'] - df_main['qtd_recebida']  
    
    df_tracer = tracer.read_stagging_data().drop_missplaced_elements().get_report()
    df_tracer = df_tracer.loc[df_tracer['cwp'].isin(df_main['cwp'].drop_duplicates(keep='first'))]
    df_main = df_main.loc[df_main['cwp'].isin(df_tracer['cwp'].drop_duplicates(keep='first'))]

    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_main[['cwp', 'tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_faltante', 'data_inicio', 'peso_un']],
        on=['cwp', 'tag'],
        how='left'
    )  

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_codeme, axis=1)
    for item in os.listdir(ifc_dir):
        print('Processing file: ', item)
        try:
            ifc = ifcopenshell.open(os.path.join(ifc_dir, item))
            file_name = item.replace('.ifc', '')         
            for idx, row in  df_tracer.loc[df_tracer['file_name'] == file_name].iterrows():
                row = row.fillna(0)
                row['data_inicio'] = '' if row['data_inicio'] == 0 else row['data_inicio']
                row['status'] = '' if row['status'] == 0 else row['status']
                try:
                    element = ifc.by_guid(row['agg_id'])
                    pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum")
                    ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                        'Status de Entrega': str(row['status']),
                        'Início de Montagem': str(row['data_inicio']).split(' ')[0]
                    })
                except:
                    print(f'Error processing Id {row["agg_id"]}')
        except:
            print(f'Unable to read file{item}')
        ifc.write(os.path.join(output_dir, file_name + '_edited.ifc'))  


def capanema():
    output_dir = os.environ['FEDERATED_PATH_CAPANEMA']
    ifc_dir = os.environ['IFC_PATH_CAPANEMA']

    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = LX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
    reports = Reports(os.environ['REPORTS_PATH_CAPANEMA'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_CAPANEMA'])
    df_lx = lx.get_report()

    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])

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
    
    df_tracer = tracer.read_stagging_data().drop_missplaced_elements().get_report()
    df_tracer = df_tracer.loc[df_tracer['cwp'].isin(df_main['cwp'].drop_duplicates(keep='first'))]
    df_main = df_main.loc[df_main['cwp'].isin(df_tracer['cwp'].drop_duplicates(keep='first'))]

    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_main[['cwp', 'tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_faltante', 'data_inicio', 'peso_un']],
        on=['cwp', 'tag'],
        how='left'
    )  

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_sinosteel, axis=1)
    for item in os.listdir(ifc_dir):
        if 'CWP-015' not in item:
            print('Processing file: ', item)
            try:
                ifc = ifcopenshell.open(os.path.join(ifc_dir, item))
                file_name = item.replace('.ifc', '')         
                for idx, row in  df_tracer.loc[df_tracer['file_name'] == file_name].iterrows():
                    row = row.fillna(0)
                    row['data_inicio'] = '' if row['data_inicio'] == 0 else row['data_inicio']
                    row['status'] = '' if row['status'] == 0 else row['status']
                    try:
                        element = ifc.by_guid(row['agg_id'])
                        pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum")
                        ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                            'Status de Entrega': str(row['status']),
                            'Início de Montagem': str(row['data_inicio']).split(' ')[0]
                        })
                    except:
                        print(f'Error processing Id {row["agg_id"]}')
            except:
                print(f'Unable to read file{item}')
            ifc.write(os.path.join(output_dir, file_name + '_edited.ifc'))  


def vcad():
    output_dir = os.environ['OUTPUT_FEDERADO_CAPANEMA'] 

    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = LX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
    reports = Reports(os.environ['REPORTS_PATH_CAPANEMA'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_CAPANEMA'])
    vcad = Vcad(os.environ['VCAD_PATH_CAPANEMA'])
    
    df_lx = lx.get_report()

    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])
    df_lx = df_lx.loc[df_lx['supplier'].str.contains('CODEME', na=False) | df_lx['supplier'].str.contains('SINOSTEEL', na=False)]

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
    
    df_vcad = vcad.get_report()
    df = pd.merge(
        df_vcad,
        df_tracer[['IfcId', 'file_name', 'supplier', 'name', 'tag', 'cwp', 'cod_ativo', 'qtd_recebida', 'status', 'agg_id']],
        on='IfcId',
        how='left'
    )

    df.to_parquet(os.path.join(output_dir, 'vcad.parquet'), index=False)