import ifcopenshell
import ifcopenshell.geom
import ifcopenshell.util.placement
import ifcopenshell.api
import os
import pandas as pd


data_file = r"C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\01. Dashboards Ativos\Fornecimento\Sinosteel\tracer_data.parquet"
source_dir = r"C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\IFC Editados"
output_folder = r"C:\Users\EmmanuelSantana\OneDrive - VERUM PARTNERS\Projetos"
choosen_files = ['068-1220CF-08', '068-1220CF-09', '069-1220CF-01', '069-1220CF-02', '074-1220CF-02', '073-1220CF-01']


df = pd.read_parquet(
    data_file,
    columns=['file_name', 'assembly_id', 'qtd_recebida', 'qtd_lx', 'status', 'qtd_faltante', 'data_inicio']
)
print(df['file_name'].drop_duplicates())
df = df.drop_duplicates(subset=['assembly_id'])
df = df.loc[df['file_name'].isin(choosen_files)]



for item in os.listdir(source_dir):
    item_name = item.replace('.ifc', '')
    if item_name in choosen_files:
        print(item)
        item_path = os.path.join(source_dir, item)
        if os.path.isfile(item_path):
            ifc = ifcopenshell.open(item_path)
            # print(df.loc[df['file_name'] == item_name])
            for idx, row in  df.loc[df['file_name'] == item_name].iterrows():
                row = row.fillna(0)
                row['data_inicio'] = '' if row['data_inicio'] == 0 else row['data_inicio']
                row['status'] = '' if row['status'] == 0 else row['status']
                element = ifc.by_guid(row['assembly_id'])
                pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum")
                ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                    'Status de Entrega': str(row['status']),
                    'Início de Montagem': str(row['data_inicio']).split(' ')[0]
                })


            ifc.write(os.path.join(output_folder, item_name + '_edited.ifc'))  

