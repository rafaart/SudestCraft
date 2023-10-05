import pandas as pd
import os
import ifcopenshell
import ifcopenshell.api
from openpyxl import load_workbook

dir = r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\4D\Status_Federado\CF-S1985-006\Global_ID"
spf = r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\4D\Status_Federado\ReportsDownload\FED_Consolidada.xlsx"
output_dir = dir

cwp = []

# Lista todos os arquivos com a extensão .xlsx no diretório
arquivos_excel = [f for f in os.listdir(dir) if f.endswith('.xlsx')]
# Lista todos os arquivos com a extensão .ifc no diretório
arquivos_ifc = [f for f in os.listdir(dir) if f.endswith('.ifc')]

for arquivo in arquivos_ifc:
    cwp.append(arquivo.split('_')[0])

print(cwp)
print(arquivos_excel)
print(arquivos_ifc)

# Itera sobre os arquivos
for arquivo in arquivos_excel:
    if 'SPF-SPC' in arquivo:
        # Constrói o caminho completo para o arquivo
        caminho_SPF = os.path.join(dir, arquivo)
        print(caminho_SPF)
    elif 'GlobalID' in arquivo:
        # Constrói o caminho completo para o arquivo
        caminho_Navis = os.path.join(dir, arquivo)
    elif 'Componentes' in arquivo:
        # Constrói o caminho completo para o arquivo
        caminho_Componentes = os.path.join(dir, arquivo)

# Lê o arquivo em um DataFrame
df_spf = pd.read_excel(caminho_SPF)
df_navis = pd.read_excel(caminho_Navis)
df_componentes = pd.read_excel(caminho_Componentes)

# retira colunas desnecessárias
df_navis = df_navis[['GLOBAL ID', 'Name', 'IfcGUID']]
df_componentes = df_componentes[['Component UID', 'HxGNBR_StringAttribute1', 'HxGNBR_StringAttribute2', 'IWP Purpose', 'IWP Name']]
df_spf = df_spf[['GLOBAL ID', 'Unnamed: 6', 'SC 3D Name', 'SC 3D UID', 'SPF 3D Name']]

# renomear colunas
df_navis = df_navis.rename(columns={
        'GLOBAL ID': 'ifcID lower navis',
        'Name': 'tag navis',
        'IfcGUID': 'ifcID navis'
    })

df_componentes = df_componentes.rename(columns={
        'Component UID': 'UID componente', 
        'HxGNBR_StringAttribute1': 'CWP componente', 
        'HxGNBR_StringAttribute2': 'tag componente', 
        'IWP Purpose': 'IWP Purpose componente', 
        'IWP Name': 'IWP componente'
    })

df_spf = df_spf.rename(columns={
        'GLOBAL ID': 'ifcID lower spf',
        'Unnamed: 6': 'ifcID spf',
        'SC 3D Name': 'tag spf',
        'SC 3D UID': 'UID spf',
        'SPF 3D Name': 'CWP spf'
    })

# print(df_spf)
# print(df_navis)
# print(df_componentes)


# Loop sobre as duas listas simultaneamente usando a função zip
for arquivo_ifc, string_cwp in zip(arquivos_ifc, cwp):
    # Filtrar o DataFrame com base na coluna 'CWP spf'
    df_filtrado = df_spf[df_spf['CWP spf'].str.contains(string_cwp)]
    
    # Verifica se o DataFrame filtrado não está vazio
    if not df_filtrado.empty:
        print(f"Processando Arquivo IFC: {arquivo_ifc}")
        print("O merge do ifcID do NAVIS com o ifcID do SPF foi feito na fonte, caso haja atualização da mesma pode ser necessário fazer isso no código colocando as duas colunas em ordem alfabetica para correlacionar")
        # junta os DataFrames pelos valores iguais nas colunas desejadas
        df_consolidada = pd.merge(df_filtrado, df_componentes, left_on='UID spf', right_on='UID componente', how='inner')
        # print(df_consolidada.head())
        

        try:
            ifc = ifcopenshell.open(os.path.join(dir, arquivo_ifc))
            file_name = arquivo_ifc.replace('.ifc', '')         
            for idx, row in  df_consolidada[['ifcID spf', 'IWP componente']].iterrows():
                try:
                    element = ifc.by_guid(row['ifcID spf'])
                    pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum_iwp")
                    ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                        'IWP': str(row['IWP componente']),
                                            })
                except:
                    print(f'Error processing Id {row["ifcID spf"]}')
        except:
            print(f'Unable to read file {arquivo_ifc}')
        ifc.write(os.path.join(output_dir, file_name + '_edited.ifc'))  
    else:
        print(f"Nenhuma correspondência encontrada para a string CWP: {string_cwp}")

# for item in os.listdir(dir):
#         if any(cwp_file in item for cwp_file in item):
#             print('Processing file: ', item)

# item = "CF-S1985-008-S-MT-CWP-730_ED-1230CF-01.ifc"
# try:
#     ifc = ifcopenshell.open(os.path.join(ifc_dir, item))
#     file_name = item.replace('.ifc', '')         
#     for idx, row in  df_spf[['iFCguid_NAVIS', 'IWP']].iterrows():
#         try:
#             element = ifc.by_guid(row['iFCguid_NAVIS'])
#             pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum_iwp")
#             ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
#                 'IWP': str(row['IWP']),
#                                     })
#         except:
#             print(f'Error processing Id {row["iFCguid_NAVIS"]}')
# except:
#     print(f'Unable to read file {item}')
# ifc.write(os.path.join(output_dir, file_name + '_edited.ifc'))  


