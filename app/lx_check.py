from data_sources.LX import LX

lx_sinosteel = LX(r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\SMAT\LX\SINOSTEEL\LX_GERAL_SINOSTEEL')
lx_sinosteel.config['depth'] = 0
lx_sinosteel.config['ignore'] = ['.SUPERADOS', 'old', 'ETL', 'ROMANEIOS']
lx_sinosteel._map_files()
lx_sinosteel._read_files()


df = lx_sinosteel.df_raw
df = df.rename(columns={
    'CWP': 'cwp',
    'TAG DA REFERÊNCIA': 'cod_ativo',
    'CÓDIGO DO MATERIAL (SKU)': 'tag',
    'DESCRIÇÃO COMPLETA': 'descricao',
    'QTDE': 'qtd_lx', 
    'PESO UNIT(KG)': 'peso_un_lx',
    'OBS': 'obs'
})
df = df.dropna(subset=['cwp', 'tag', 'qtd_lx'])
df['cwp'] = df['cwp'].str.replace(' ', '')
df['tag'] = df['tag'].str.replace(' ', '')

df['peso_un_lx'] = df['peso_un_lx'].apply(lambda x: round(float(x), 2))
df = df.drop_duplicates(subset=['cwp', 'tag', 'peso_un_lx'], keep=False)
duplicated = df.loc[df.duplicated(subset=['cwp', 'tag'], keep=False)]
duplicated = duplicated[['cwp','cod_ativo','tag','descricao','qtd_lx','peso_un_lx','supplier','file_name']]
duplicated.to_excel(r"C:\Users\EmmanuelSantana\Downloads\Itens duplicados com erro de peso.xlsx", index=False)
print(duplicated)