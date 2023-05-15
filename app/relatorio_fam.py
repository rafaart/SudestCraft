import pandas as pd
import suppliers
import os
from materials import Reports
from bim_tools import TracerFullReport

from dotenv import load_dotenv
from config.config import run_config
load_dotenv('.env')
run_config()

producao = suppliers.ProducaoFAM(os.environ['PRODUCAO_PATH_NEWSTEEL'])
reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])

df_producao = producao.get_report()
df_desenho = reports.get_status_desenho()
df_recebimento = reports.get_recebimento()

df_producao['chave'] = df_producao['chave'].replace(' ', '')

df_desenho['cwa'] = df_desenho['cwp'].str.split('-').str[2].str.zfill(3)
df_desenho['tag'] = df_desenho['tag'].replace(' ', '')
df_desenho['chave'] = 'CWA' + df_desenho['cwa'] + '-' + df_desenho['tag']

df = pd.merge(
    left=df_producao,
    right=df_desenho, 
    on='chave',
    how='left',
    suffixes=('_romaneio', '_desenho')
)

df['peso_total_romaneio'] = df['qtd_total'] * df['peso_un_romaneio']
df['peso_total_desenho'] = df['qtd_desenho'] * df['peso_un_desenho']
df = df.rename(columns={'tag_desenho': 'tag', 'qtd_total': 'qtd_romaneio'})
df.loc[df['peso_total_desenho'] != df['peso_total_romaneio'], 'error'] = True
df['percentual_diferença'] = (df['peso_total_romaneio'] - df['peso_total_desenho']) / df['peso_total_romaneio']
df['percentual_diferença'] = df['percentual_diferença'].abs()
df[[
    'cwp',
    'tag', 
    'qtd_romaneio',
    'peso_un_romaneio',
    'peso_total_romaneio',
    'qtd_desenho',
    'peso_un_desenho',
    'peso_total_desenho',
    'percentual_diferença',
    'error',
]].to_excel('fam_report.xlsx', index=False)
