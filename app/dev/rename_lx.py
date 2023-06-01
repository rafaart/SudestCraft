import os
import pandas as pd
import numpy as np

root =r'C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\LX\LX Geral'
for item in os.listdir(root):
    file_path = os.path.join(root, item)
    wb = pd.ExcelFile(file_path)
    [cover_sheet] = [sheet_name for sheet_name in wb.sheet_names if 'Rosto' in sheet_name or 'Capa' in sheet_name]
    work_sheet = wb.parse(
        work_sheet=cover_sheet
    )

    rev = work_sheet.iloc[6,13]
    try:
        if not (isinstance(rev, str) or isinstance(rev, int)):
            rev = work_sheet.iloc[5,14]
    
        if not (isinstance(rev, str) or isinstance(rev, int)):
            rev = work_sheet.iloc[6,14]
    except:
        rev = work_sheet.iloc[4,13]
        
    wb.close()
    old_name, ext = item.split('.')
    new_name = os.path.join(root, f'{old_name}_REV_{rev}.{ext}' )
    
    os.rename(file_path, new_name)
