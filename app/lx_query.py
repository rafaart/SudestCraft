import os
import pandas as pd
from suppliers import SuppliersLX

from dotenv import load_dotenv
from config.config import run_config
load_dotenv('.env')
run_config()


lX_dir_newsteel = SuppliersLX(os.environ['LX_PATH_NEWSTEEL'], os.environ['MAPPER_PATH_NEWSTEEL'])
lX_dir_capanema = SuppliersLX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])

df_capanema = lX_dir_capanema.get_report()
df_error = lX_dir_capanema.get_erros()
print(df_capanema.loc[df_capanema['tag'].str.contains('0042A'), ['file_name', 'supplier']].drop_duplicates(keep='first'))
# print(df_error)
# print(df_capanema)
