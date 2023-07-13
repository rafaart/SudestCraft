import pandas as pd
import os

class Masterplan:
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'dd' in item.lower():
                    self.df_raw = pd.read_excel(
                        os.path.join(source_dir, item),
                        usecols=['Activity ID', 'BL Start', 'BL Finish', 'Start', 'Finish']
                    )
                    break
    
    def get_report(self):
        self._clean_report()
        return self.df_report
            
    def _clean_report(self):
        df = self.df_raw
        df = df.rename(columns={
            'Activity ID': 'cwp', 
            'BL Start': 'data_inicio_baseline', 
            'BL Finish': 'data_termino_baseline', 
            'Start': 'data_inicio', 
            'Finish': 'data_termino', 
        })
        df.loc[df['cwp'].str.lower().str.contains('MT'), 'sheet_name'] = 'montagem' 
        df.loc[~df['cwp'].str.lower().str.contains('MT'), 'sheet_name'] = 'civil'
        df = df.loc[df['cwp'].str.contains('-CWP')]
        df['cwp'] = df['cwp'].str.replace(' ', '')
        df['data_inicio_baseline'] = df['data_termino_baseline'].dt.date
        self.df_report = df


class MasterplanDD:
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'dd' in item.lower():
                    worksheets = []
                    workbook = pd.ExcelFile(os.path.join(source_dir, item))
                    for sheet in workbook.sheet_names:
                        worksheet = self.__class__._rename_cols(workbook.parse(sheet))
                        worksheet['sheet_name'] = sheet
                        worksheets.append(worksheet)
                    df_workbook = pd.concat(worksheets, axis=0, ignore_index=True)
                    self.df_workbook = self._apply_default_sheet_name(df_workbook)
                    break
    
    def get_report(self):
        return self._clean_report(self.df_workbook)

    def _apply_default_sheet_name(self, df):
        df.loc[df['sheet_name'].str.lower().str.contains('mont'), 'sheet_name'] = 'montagem' 
        df.loc[df['sheet_name'].str.lower().str.contains('civil'), 'sheet_name'] = 'civil'
        return df
    
    def _clean_report(self, df):
        df.loc[~df['cwp'].str.contains('-CWP'), 'cwp'] = df['cwp'] + '-CWP'
        df['cwa'] = df['cwp'].str.split('-').str[2]
        return df

    @staticmethod
    def _rename_cols(df):
        cols_mapper = {col:None for col in df.columns}
        for key in cols_mapper.keys():
            if 'código cwp' in key.lower():
                cols_mapper[key] = 'cwp'
            elif 'código cwa' in key.lower():
                cols_mapper[key] = 'cwa'
            elif 'início' in key.lower():
                cols_mapper[key] = 'data_inicio'
            elif 'término' in key.lower():
                cols_mapper[key] = 'data_termino'
            elif 'descrição cwp' in key.lower():
                cols_mapper[key] = 'descricao_cwp'
            elif 'descrição cwa' in key.lower():
                cols_mapper[key] = 'descricao_cwa'
        return df.rename(columns=cols_mapper)


class ListaMaster:
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'lista de cwas' in item.lower():
                    self.df_workbook = pd.read_excel(
                        os.path.join(source_dir, item),
                        sheet_name='CWPs - EWPs - PWPs',
                        skiprows=8,
                        usecols=['Nº CWA', 'DESCRIÇÃO CWA', 'CWP', 'DESCRIÇÃO DO CODE', 'EWP ASSOCIADO', 'DESCRIÇÃO ATIVIDADE EWP']
                    )
                    break
    
    def get_report(self):
        return self._clean_report(self.df_workbook)
    
    def _clean_report(self, df):
        df = df.rename(columns={
            'Nº CWA': 'cwa',
            'DESCRIÇÃO CWA': 'descricao_cwa', 
            'CWP': 'cwp', 
            'DESCRIÇÃO DO CODE': 'descricao_cwp', 
            'EWP ASSOCIADO': 'ewp', 
            'DESCRIÇÃO ATIVIDADE EWP': 'descricao_ewp'
        })
        return df