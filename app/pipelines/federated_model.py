import pandas as pd
import suppliers
import os
import pipeline_tools
from materials import Reports
from bim_tools import TracerFullReport
from masterplan import Masterplan
import ifcopenshell
import ifcopenshell.api

pd.options.mode.chained_assignment = None

def capanema():
    output_dir = r'C:\Users\EmmanuelSantana\Documents\Nova pasta'
    ifc_dir = r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\IFC Editados'

    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = suppliers.SuppliersLX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
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
        if 'CF-S1985-008-S-MT-CWP-731-ED-1360CF-01' in item:
            print('Processing file: ', item)
            ifc = ifcopenshell.open(os.path.join(ifc_dir, item))
            for element in ifc.by_type('IfcMechanicalFastener'):
                ifc.remove(element)
            # for element in ifc.by_type('IfcElement'):
            #     if str(element.Name) in ['TELHADO', 'RUFO', 'CALHA']:
            #         ifc.remove(element)
            file_name = item.replace('.ifc', '')         
            for idx, row in  df_tracer.loc[df_tracer['file_name'] == file_name].iterrows():
                row = row.fillna(0)
                row['data_inicio'] = '' if row['data_inicio'] == 0 else row['data_inicio']
                row['status'] = '' if row['status'] == 0 else row['status']
                element = ifc.by_guid(row['assembly_id'])
                pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum")
                ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                    'Status de Entrega': str(row['status']),
                    'Início de Montagem': str(row['data_inicio']).split(' ')[0]
                })
            ifc.write(os.path.join(output_dir, file_name + '_edited.ifc'))  

