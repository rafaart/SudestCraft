import os
import pandas as pd
from suppliers import SuppliersLX

from dotenv import load_dotenv
from config.config import run_config
load_dotenv('.env')
run_config()

pd.set_option('display.max_rows', None)

lX_dir_newsteel = SuppliersLX(os.environ['LX_PATH_NEWSTEEL'], os.environ['MAPPER_PATH_NEWSTEEL'])
lX_dir_capanema = SuppliersLX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])

df_capanema = lX_dir_capanema.get_report()
df_error = lX_dir_capanema.get_erros()
print(df_capanema.columns)
print(df_capanema.loc[
    df_capanema['tag'].str.contains('C2132')
])


# df_newsteel = lX_dir_newsteel.get_report()
# df_error = lX_dir_newsteel.get_erros()
# print(df_newsteel.columns)
# print(df_newsteel.loc[
#     df_newsteel['cwp'].str.contains('VG-P0400-036'), 
#     ['file_name', 'cwp']    
# ])