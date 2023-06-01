import os
import pandas as pd
import xlrd
from openpyxl import load_workbook



file_path = r'C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\SMAT\LX\TECMA\CF-S1985-003-M-MT-CWP-835_DF-LX-1000CF-T-00002-T-00001_TECMA\REV_A\DF-LX-1000CF-T-00002-T-00001.xls'
# wb = load_workbook(file_path)
# new_name=file_path.replace('.xls', '_.xls')
# wb.save(new_name)
wb = pd.ExcelFile(file_path)
for sheet in wb.sheet_names:
    df = wb.parse(sheet)
# print(df)