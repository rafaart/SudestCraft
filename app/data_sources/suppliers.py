import pandas as pd
import os
from openpyxl import load_workbook
from datetime import datetime

class MemoriaCalculo():
    def __init__(self, source_dir) -> None:
        items = os.listdir(source_dir)
        m_dates = [os.path.getmtime(os.path.join(source_dir, item)) for item in items]
        for item, _ in sorted(zip(items, m_dates), key=lambda pair: pair[1], reverse=True):  
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'CapEx Engenharia - AWP' in item:
                    print('File used for PQ report: ', item)
                    self.df_scheduler = pd.read_excel(
                        os.path.join(source_dir, item),
                        sheet_name='Geral - Verum OFICIAL',
                        skiprows=10,
                        usecols=['CWP', 'QUANT. DETALHADO CORRIGIDA', 'UNID CORRIGIDA', 'Fornecedor', 'K']
                    )
                break

    def get_report(self):
        self._clean_report()
        return self.report
    

    def _clean_report(self):
        df = self.df_scheduler
        df = df.rename(columns={
            'CWP': 'cwp', 
            'QUANT. DETALHADO CORRIGIDA': 'peso_capex_ton',
            'UNID CORRIGIDA': 'un',
            'Fornecedor': 'fornecedor',
            'K': 'cod_atividade',
        })
        df.loc[df['fornecedor'].str.contains('SINOSTEEL', na=False), 'fornecedor'] = 'SINOSTEEL'
        df['fornecedor'] = df['fornecedor'].fillna('')
        df = df.loc[(df['un']=='t') | (df['un']=='kg')]
        df = df.loc[pd.to_numeric(df['peso_capex_ton'], errors='coerce').notnull()]
        df.loc[df['un']=='kg', 'peso_capex_ton'] = df['peso_capex_ton'] / 1000
        self.cwp_desmontagem = df.loc[df['cod_atividade'] == 'D']
        df = df.loc[df['cod_atividade'] != 'D']
        df = df.groupby(by=['cwp', 'fornecedor'], as_index=False).sum(numeric_only=True)
        self.report = df
        return self
        
         

class PQSimplified():
    def __init__(self, source_dir) -> None:
        items = os.listdir(source_dir)
        m_dates = [os.path.getmtime(os.path.join(source_dir, item)) for item in items]
        for item, _ in sorted(zip(items, m_dates), key=lambda pair: pair[1]):  
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'pq' in item.lower():
                    self.df_scheduler = pd.read_excel(
                        os.path.join(source_dir, item),
                        sheet_name='Planilha1',
                        usecols=['CWP', 'Peso total (t)', 'Peso total (kg)']
                    )

    def get_report(self):
        return self.__class__._clean_report(self.df_scheduler)
    
    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'CWP': 'cwp', 
            'Peso total (t)': 'peso_total_t',
            'Peso total (kg)': 'peso_total_kg',
        })
        return df
         

class LX():
    def __init__(self, source_dir, grouped_by_building=False) -> None:
        self.source_dir = source_dir
        self.grouped_by_building = grouped_by_building
        self.config = {
            'extensions': ['.xls', '.xlsx'],
            'ignore':['.SUPERADOS', 'old', 'ETL'],
            'depth':2
        }

    def get_report(self):
        self._run_pipeline()
        return self.df_lx

    def get_erros(self):
        self._run_pipeline()
        return self.df_errors

    def _run_pipeline(self):
        self._map_files()
        self._read_files()
        self._clean_raw()

    def _map_files(self):
        def filter_dir(dir_path):
            return dir_path[len(self.source_dir):].count(os.sep) > self.config['depth'] and all(term not in dir_path for term in self.config['ignore'])

        def filter_files(file_name):
            return any(ext in file_name for ext in self.config['extensions'])

        mapped_dirs = [(root, files) for root, _, files in os.walk(self.source_dir) if filter_dir(root)]
        self.mapped_files = [os.path.join(dir_path, file_name) for dir_path, files in mapped_dirs for file_name in files if filter_files(file_name)]

        
    def _read_files(self):
        worksheets = []
        df_errors = pd.DataFrame(columns=['file', 'sheet'])
        for file_path in self.mapped_files:
            try:
                mod_date = os.path.getmtime(file_path)
                workbook = pd.ExcelFile(file_path)
                for sheet in workbook.sheet_names:
                    if 'rosto' in sheet.lower():
                        worksheet = workbook.parse(
                            sheet,
                            skiprows=2,
                        )
                        rev = worksheet.iloc[4,13]
                        cod_vale = worksheet.iloc[4,9]
                        if 'LX' not in str(cod_vale):
                            cod_vale = worksheet.iloc[0,9]
                            if 'CF' not in str(cod_vale) and 'LX' not in str(cod_vale):
                                cod_vale = worksheet.iloc[0,10]
                                if 'CF' not in str(cod_vale) and 'LX' not in str(cod_vale):
                                    cod_vale = worksheet.iloc[0,12]
                    if 'rosto' not in sheet.lower():
                        try:
                            worksheet = workbook.parse(
                                sheet,
                                skiprows=2,
                                na_values=[' ', 'D', '-', '1\n1'],
                                usecols=['CWP', 'PESO UNIT(KG)', 'CÓDIGO DO MATERIAL (SKU)', 'QTDE', 'DESCRIÇÃO COMPLETA', 'OBS', 'TAG DA REFERÊNCIA'],
                                converters={
                                    'CWP': str, 
                                    'CÓDIGO DO MATERIAL (SKU)': str, 
                                    'DESCRIÇÃO COMPLETA': str,  
                                    'OBS': str,  
                                    'TAG DA REFERÊNCIA': str,
                                    'PESO UNIT(KG)': str,
                                }
                            )
                            worksheet['supplier'] = file_path[len(self.source_dir):].split(os.sep)[1]
                            worksheet['rev'] = str(rev)
                            worksheet['cod_vale'] = cod_vale
                            worksheet['sheet_name'] = sheet
                            worksheet['file_name'] = os.path.basename(file_path)
                            worksheet['file_path'] = file_path
                            worksheet['last_mod_date'] = datetime.fromtimestamp(mod_date).strftime('%Y-%m-%d %H:%M:%S')
                            worksheets.append(worksheet)
                        except:
                            row = pd.DataFrame({'file': [os.path.basename(file_path)], 'sheet': [sheet]})
                            df_errors = pd.concat([df_errors, row], axis=0, ignore_index=True)          
            except Exception as e:
                row = pd.DataFrame({'file': [os.path.basename(file_path)], 'sheet': None})
                df_errors = pd.concat([df_errors, row], axis=0, ignore_index=True)                
        self.df_raw = pd.concat(worksheets, axis=0, ignore_index=True)
        self.df_errors = df_errors

    def _clean_raw(self):
        df = self.df_raw
        df = df.rename(columns={
            'CWP': 'cwp',
            'TAG DA REFERÊNCIA': 'cod_ativo',
            'CÓDIGO DO MATERIAL (SKU)': 'tag',
            'DESCRIÇÃO COMPLETA': 'descricao',
            'QTDE': 'qtd_lx', 
            'PESO UNIT(KG)': 'peso_un',
            'OBS': 'obs'
        })
        df = df.dropna(subset=['cwp', 'tag', 'qtd_lx'])
        df['cwp'] = df['cwp'].str.replace(' ', '')
        df['tag'] = df['tag'].str.replace(' ', '')
        
        df.loc[~df['obs'].str.contains('NÃO MODELAD', na=False), 'geometry'] = True
        df.loc[df['obs'].str.contains('NÃO MODELAD', na=False), 'geometry'] = False

        df['cwp_number'] = df['cwp'].str.split('-').str[-1]
        df['chave'] = df['cwp_number'].str.zfill(3) + '-' + df['tag']
        df.loc[df['peso_un'].str.contains(',', na=False, regex=False), 'peso_un'] = df['peso_un'].str.replace('.', '').str.replace(',', '.')
        df['peso_un'] = df['peso_un'].apply(lambda x: 0 if '-' in str(x) else float(x))
        df['qtd_lx'] = df['qtd_lx'].astype(float)

        keys = ['cwp', 'cod_ativo', 'tag'] if self.grouped_by_building else ['cwp', 'tag']
        df_numeric = df[keys + ['qtd_lx']].groupby(by=keys, as_index=False).sum(numeric_only=True).round(0)
        df_categorical = df.copy().drop(columns=['qtd_lx']).drop_duplicates(subset=keys, keep='first')
        df = pd.merge(df_numeric, df_categorical, how='left', on=keys)
        df = df.sort_values(by=['rev'], ascending=False).drop_duplicates(subset=['cod_vale', 'cwp', 'tag'], keep='first')

        self.df_lx = df


class CronogramaMasterConstrucap:
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'construcap' in item.lower():
                    self.df_workbook = pd.read_excel(
                        os.path.join(source_dir, item),
                        usecols=['Item', 'P0400: cwa_id', "P0400: CWP's / EWP's / PWP's_Work_Type", 'Activity Name', 'Start', 'Finish']
                    )
                    break
    
    def get_report(self):
        return self.__class__._clean_report(self.df_workbook)

    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'Item': 'item',
            'P0400: cwa_id': 'cwa',
            "P0400: CWP's / EWP's / PWP's_Work_Type": 'cwp',
            'Activity Name': 'descricao',
            'Start': 'data_inicio',
            'Finish': 'data_termino'
        })
        df = df.loc[df['item'].str.count('.') > 2]
        df = df.dropna(subset=['cwp'])
        df.loc[df['cwp'].str.contains('-CWPp'), 'atividade'] = 'pre-montagem'
        df.loc[df['cwp'].str.contains('-CWPm'), 'atividade'] = 'montagem'
        df['cwp'] = df['cwp'].str.split('-CWP').str[0] + '-CWP'
        df_min = df.loc[df.groupby('cwp', sort=False)['data_inicio'].idxmin()].drop(columns=['data_termino'])
        df_max = df.loc[df.groupby('cwp', sort=False)['data_termino'].idxmax(), ['cwp', 'data_termino']]
        df = pd.merge(
            left=df_min,
            right=df_max,
            on='cwp',
            how='outer'
        )
        return df



class Construcap():
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'cronograma construcap' in item.lower():
                    self.df_scheduler = pd.read_excel(
                        os.path.join(source_dir, item),
                        sheet_name='Datas início montagem CWP',
                        usecols=['CWA', 'Descrição CWA', 'CWP Montagem', 'Descrição CWP Montagem', 'Data de início de montagem - Real / Tendência Montadora']
                    )

    def get_report(self):
        return self.__class__._clean_report(self.df_scheduler)
    
    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'CWA': 'cwa', 
            'Descrição CWA': 'descricao_cwa', 
            'CWP Montagem': 'cwp', 
            'Descrição CWP Montagem': 'descricao_cwp', 
            'Data de início de montagem - Real / Tendência Montadora': 'data_inicio'
        })
        df.loc[~df['cwp'].str.contains('-CWP'), 'cwp'] = df['cwp'] + '-CWP'
        return df


class Cardan():
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'cronograma cardan' in item.lower():
                    self.df_scheduler = pd.read_excel(
                        os.path.join(source_dir, item),
                        usecols=['CWA', 'CWP', 'Data de início CWP  - Real / Tendência  Construtora', 'Data de término CWP  - Real / Tendência  Construtora', 'Avanço Previsto', 'Avanço Real']
                    )

    def get_report(self):
        return self.__class__._clean_report(self.df_scheduler)
    
    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'CWA': 'cwa', 
            'CWP': 'cwp', 
            'Data de início CWP  - Real / Tendência  Construtora': 'data_inicio',
            'Data de término CWP  - Real / Tendência  Construtora': 'data_termino',
        })
        df.loc[~df['cwp'].str.contains('-CWP'), 'cwp'] = df['cwp'] + '-CWP'
        return df


class Aumond():
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'aumond' in item.lower():
                    workbook = pd.ExcelFile(os.path.join(source_dir, item))
                    for sheet_name in workbook.sheet_names:
                        if 'aumond' in sheet_name.lower():
                            self.df_scheduler = workbook.parse(
                                sheet_name=sheet_name,
                            )
                            break

    def get_report(self):
        return self.__class__._clean_report(self.df_scheduler)
    
    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'Start': 'data_inicio', 
            'Finish': 'data_termino', 
            'PWP (PWPe/PWP/PWPl)': 'cwp'
        })
        df = df.loc[df['cwp'].str.contains('-PWPl', na=False)]
        df['cwp'] = df['cwp'].str.replace('-PWPl', '')
        df.loc[~df['cwp'].str.contains('-CWP'), 'cwp'] = df['cwp'] + '-CWP'
        return df


class FamStructure():
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'fam estrutura' in item.lower():
                    workbook = pd.ExcelFile(os.path.join(source_dir, item))
                    for sheet_name in workbook.sheet_names:
                        if 'fam' in sheet_name.lower():
                            self.df_scheduler = workbook.parse(
                                sheet_name=sheet_name,
                                usecols=['Start', 'Finish', 'PWP (PWPe/PWP/PWPl)']
                            )
                            break

    def get_report(self):
        return self.__class__._clean_report(self.df_scheduler)
    
    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'Start': 'data_inicio', 
            'Finish': 'data_termino', 
            'PWP (PWPe/PWP/PWPl)': 'cwp'
        })
        df = df.loc[df['cwp'].str.contains('-PWPl', na=False)]
        df['cwp'] = df['cwp'].str.replace('-PWPl', '')
        df.loc[~df['cwp'].str.contains('-CWP'), 'cwp'] = df['cwp'] + '-CWP'
        return df



class FamMining():
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'fam mining' in item.lower():
                    workbook = pd.ExcelFile(os.path.join(source_dir, item))
                    for sheet_name in workbook.sheet_names:
                        if 'fam' in sheet_name.lower():
                            self.df_scheduler = workbook.parse(
                                sheet_name=sheet_name,
                                usecols=['Start', 'Finish', 'PWP (PWPe/PWP/PWPl)', 'Activity Name']
                            )
                            break

    def get_report(self):
        return self.__class__._clean_report(self.df_scheduler)
    
    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'Start': 'data_inicio', 
            'Finish': 'data_termino', 
            'PWP (PWPe/PWP/PWPl)': 'cwp',
            'Activity Name': 'descricao'
        })
        df = df.loc[df['cwp'].str.contains('-PWPl', na=False)]
        df = df.loc[df['descricao'].str.contains('Logística da CWA', na=False)]
        df['cwp'] = df['cwp'].str.replace('-PWPl', '')
        df.loc[~df['cwp'].str.contains('-CWP'), 'cwp'] = df['cwp'] + '-CWP'
        df = df.drop(columns=['descricao'])
        return df


class ModeloCronogramaCapanema():
    def __init__(self, file_name, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if file_name in item.lower():
                    self.df_scheduler = pd.read_excel(
                        os.path.join(source_dir, item),
                        usecols=['PWP', 'Nome da Tarefa', 'Início', 'Término', 'Início da Linha de Base', 'Término da Linha de Base']
                    )
                    break
    def get_report(self):
        return self.__class__._clean_report(self.df_scheduler)
    
    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'PWP': 'pwp', 
            'Nome da Tarefa': 'descricao', 
            'Início': 'data_inicio', 
            'Término': 'data_termino',  
            'Início da Linha de Base': 'data_inicio_base',
            'Término da Linha de Base': 'data_termino_base',
        })

        df['cwp'] = df['pwp'].str.replace('-PWP-', '-CWP-')
        return df


class ProducaoFAM():
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'avanço fabricação' in item.lower():
                    self.df_raw = pd.read_excel(
                        os.path.join(source_dir, item),
                        sheet_name='MEMÓRIA',
                        skiprows=1,
                        usecols=['Cod Ajustada', 'Marca', 'Tipo Da Peça', 'Qtde. Total', 'Peso Unit. Strumis', 'Em Preparação', 'Preparado', 'Em Expedição', 'Qtde Expedida Obra']                  
                    )
                    break

    def get_report(self):
        self._clean_report()
        return self.df_report
    
    def _clean_report(self):
        df = self.df_raw
        df = df.rename(columns={
            'Cod Ajustada': 'cwp_short',
            'Marca': 'tag',
            'Tipo Da Peça': 'tipo',
            'Qtde. Total': 'qtd_total',
            'Peso Unit. Strumis': 'peso_un',
            'Em Preparação': 'peso_preparacao',
            'Preparado': 'peso_preparado',
            'Em Expedição': 'peso_expedicao',
            'Qtde Expedida Obra': 'qtd_entregue'
        })
        df = df.dropna(subset='tag')
        df = df.fillna(0)
        df['cwa'] = df['cwp_short'].str.replace('-', '').str.replace('CWP', 'CWA')
        df = df.loc[df['cwa'].str.contains('CWA')]
        df['chave'] = df['cwa'] + '-' + df['tag']

        df['qtd_embarque'] = (df['peso_expedicao']/ df['peso_un']) - df['qtd_entregue']
        df['qtd_embarque'] = df['qtd_embarque'].apply(lambda x: 0 if x < 0 else x)
        df['qtd_fabricacao'] = (df['peso_preparado'].astype(float) + df['peso_preparacao'] - df['peso_expedicao']) / df['peso_un']
        df['qtd_programacao'] = df['qtd_total'] - df['qtd_entregue'] - df['qtd_embarque'] - df['qtd_fabricacao'] 
        
        numeric_columns = ['qtd_entregue', 'qtd_fabricacao', 'qtd_embarque', 'qtd_programacao', 'qtd_total']
        categorical_columns = ['cwa', 'cwp_short', 'chave', 'tag', 'tipo', 'peso_un']

        df_numeric = df[numeric_columns + ['chave']].copy().groupby(by='chave', as_index=False).sum(numeric_only=True).round(0)
        df_categorical = df[categorical_columns].copy().drop_duplicates(subset='chave', keep='first')

        df = pd.merge(df_numeric, df_categorical, how='left', on='chave')
        df['total_sum'] = df[numeric_columns].sum(axis=1) - df['qtd_total']
        self.df_report = df


class ProducaoEMALTO():
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'relatório de acompanhamento' in item.lower():
                    self.df_raw = pd.read_excel(
                        os.path.join(source_dir, item),
                        skiprows=1,
                        header=0,
                        na_values=['(kg)', '~~~~~~~~~', ' '],
                        usecols=[
                            'FASE', 
                            'MARCA', 
                            'QTD', 
                            'PESO TOTAL', 
                            'PREPARAÇÃO', 
                            'MONTAGEM', 
                            'SOLDA', 
                            'ACABAMENTO', 
                            'JATO ',
                            'PINTURA', 
                            'EXPEDIÇÃO',
                            'PREV EMBARQUE',
                        ]
                    )
                    break
        else:
            raise ValueError("Relatório EMALTO não foi encontrado")

    def get_report(self):
        self._clean_report()
        return self.df_report
    
    def _clean_report(self):
        df = self.df_raw
        df = df.rename(columns={
            'FASE': 'cwa', 
            'MARCA': 'tag', 
            'QTD': 'qtd_total', 
            'PESO TOTAL': 'peso_total', 
            'PREPARAÇÃO': 'peso_preparacao', 
            'MONTAGEM': 'peso_montagem', 
            'SOLDA': 'peso_soldagem', 
            'ACABAMENTO': 'peso_acabamento', 
            'JATO ': 'peso_jateamento',
            'PINTURA': 'peso_pintura',
            'EXPEDIÇÃO': 'peso_expedicao',
            'PREV EMBARQUE': 'data_embarque',
        })
        numeric_columns = [
            'qtd_total', 
            'peso_total', 
            'peso_preparacao', 
            'peso_montagem', 
            'peso_soldagem', 
            'peso_acabamento', 
            'peso_jateamento', 
            'peso_pintura', 
            'peso_expedicao'
        ]
        df = df.dropna(axis = 0, how = 'all')
        df = df.dropna(subset=['tag'])
        df = df.loc[~df['tag'].str.contains('MARCA', na=False)]
        df = df.loc[~df['tag'].str.contains('TOTAL', na=False)]
        df[numeric_columns] = df[numeric_columns].fillna(0.)
        df = df.fillna(method='ffill')
        df['cwa'] = df['cwa'].str.split('CWA')
        df['cwa'] = df['cwa'].str[1].str[:4]
        df['cwa'] = df['cwa'].str.extract('(\d+)')
        df['cwa'] = df['cwa'].str.zfill(3)
        df['cwa'] = 'CWA'+df['cwa']
        df['cwa_number'] = df['cwa'].str.extract('(\d+)')
        
        df['peso_un'] = df['peso_total'].astype(float) / df['qtd_total'].astype(float)
        df['peso_fabricacao'] = df[['peso_montagem', 'peso_soldagem', 'peso_acabamento', 'peso_jateamento', 'peso_pintura']].max(axis=1) - df['peso_expedicao']
        df['peso_preparacao'] = df['peso_preparacao'] - df['peso_fabricacao'] - df['peso_expedicao']
        df['peso_programacao'] = df['peso_total'] - df['peso_preparacao'] - df['peso_fabricacao'] - df['peso_expedicao']

        df['qtd_programacao'] = df['peso_programacao'] / df['peso_un']
        df['qtd_preparacao'] = df['peso_preparacao'] / df['peso_un']
        df['qtd_fabricacao'] = df['peso_fabricacao'] / df['peso_un']
        df['qtd_expedicao'] = df['peso_expedicao'] / df['peso_un']

        df[['qtd_programacao', 'qtd_preparacao', 'qtd_fabricacao', 'qtd_expedicao']] = df[['qtd_programacao', 'qtd_preparacao', 'qtd_fabricacao', 'qtd_expedicao']].round(0)
        self.df_report = df


class RomaneioEMALTO():
    def __init__(self, source_dir) -> None:
        self.workbooks = []
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'romaneio padrão vale' in item.lower():
                    try:
                        file_path=os.path.join(source_dir, item)
                        wb = load_workbook(file_path, data_only=True)
                        sh = wb["LTE"]
                        cwa =sh["d5"].value
                        
                        workbook = pd.read_excel(
                            file_path,
                            skiprows=20,
                            usecols=['COD. MATERIAL', 'QTD.'],
                            dtype={'COD. MATERIAL':'object', 'QTD.':float}
                        )
                        workbook['cwa'] = cwa
                        self.workbooks.append(workbook)
                    except:
                        raise OSError(f"Unable to read file: {item}")
        try:
            self.df_raw = pd.concat(self.workbooks, axis=0, ignore_index=True) 
        except:
            raise FileNotFoundError("Source file not found")

    def get_report(self):
        self._clean_report()
        return self.df_report
    
    def _clean_report(self):
        df = self.df_raw
        df = df.rename(columns={
            'COD. MATERIAL': 'tag', 
            'QTD.': 'qtd_romaneio',
        })
        df = df.dropna(subset=['tag'])
        df = df.groupby(['cwa', 'tag'], as_index=False).sum()
        df['cwa'] = df['cwa'].str.extract('(\d+)')
        df['cwa'] = df['cwa'].str.zfill(3)
        df['cwa'] = 'CWA'+df['cwa']
        self.df_report = df