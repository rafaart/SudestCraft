import pandas as pd
import os
import ifcopenshell
import ifcopenshell.api
from openpyxl import load_workbook

ifc_dir = r"c:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\4D\Status_Federado\CF-S1985-008"
spf = r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\4D\Status_Federado\CF-S1985-008\consolidada.xlsx"
output_dir = r"c:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\4D\Status_Federado\CF-S1985-008"

df_spf = pd.read_excel(spf)
print(df_spf)
# [IfcGUID]	[Global_ID]	[SC 3D UID]	[SC 3D Name]	[IWP]	[IWP_proposito]	[Avanço]



# Criar a nova coluna "IWP_main" com os 29 primeiros caracteres de "IWP"
# df_spf['IWP_main'] = df_spf['IWP'].str[:29]

# Agrupe o DataFrame por "IWP" e "IWP_proposito" e conte a ocorrência de cada combinação
# summary = df_spf.groupby(['IWP_main', 'IWP_proposito']).size().reset_index(name='count')

# Filtrar apenas as linhas onde o valor em "IWP" se repete com valores diferentes em "IWP_proposito"
# filtered_summary = summary[summary.duplicated(subset='IWP_main', keep=False)]

# print(filtered_summary)

for item in os.listdir(ifc_dir):
        if any(cwp_file in item for cwp_file in item):
            print('Processing file: ', item)
item = "CF-S1985-008-S-MT-CWP-730.ifc"
try:
    ifc = ifcopenshell.open(os.path.join(ifc_dir, item))
    file_name = item.replace('.ifc', '')         
    for idx, row in  df_spf[['IfcGUID', 'IWP', 'IWP_proposito', 'Avanço']].iterrows():
        try:
            element = ifc.by_guid(row['IfcGUID'])
            pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum")
            
            ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                'IWP': str(row['IWP']),
                                    })
            ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                'IWP_proposito': str(row['IWP_proposito']),
                                    })
            ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                '%Avanço': str(row['Avanço']),
                                    })
            if row['Avanço']==0:
                ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                'status_Avanço': str("não iniciado"),
                                    })
            elif row['Avanço']>0 and row['Avanço']<1:
                 ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                'status_Avanço': str("em " + row['IWP_proposito']),
                                    })
            elif row['Avanço']==1:
                 if row['IWP_proposito']=="Montagem":
                    ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                    'status_Avanço': str("Montado"),
                                    })
                 else:
                    ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                    'status_Avanço': str("Pré-montado"),
                                    })
                 
        except:
            print(f'Error processing Id {row["IfcGUID"]}')
except:
    print(f'Unable to read file {item}')
ifc.write(os.path.join(output_dir, file_name + '_edited.ifc'))