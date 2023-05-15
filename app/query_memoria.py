import os
import pandas as pd
from suppliers import MemoriaCalculo

from dotenv import load_dotenv
from config.config import run_config
load_dotenv('.env')
run_config()


memoria_calculo = MemoriaCalculo(os.environ['MEMORIA_CALCULO_PATH_CAPANEMA'])
memoria_calculo._clean_report()
df_memoria = memoria_calculo.report
print(df_memoria.columns)
print(df_memoria.loc[df_memoria['cwp'].str.contains('BB-S1985-016-M-MT-CWP-695', na=False), ['peso_capex_ton']].drop_duplicates(keep='first'))