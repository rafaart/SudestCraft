import pandas as pd
import os
import json

class Reports():
    suppliers_map = [
        'SEMCO',
        'ICON',
        'RAND',
        'FAMSTEEL',
        'HAVER'
    ]
    def __init__(self, source_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\Arquivo\Report') -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if 'recebimento.csv' in item.lower():
                    self.df_recebimento = pd.read_csv(
                        os.path.join(source_dir, item),
                        encoding = 'ANSI',
                        sep=';', 
                        index_col=False,
                        header=0,
                        usecols=[' TAG/CÓDIGO ', ' QT RECEBIDA ', ' FORNECEDOR ']
                    )
                if 'desenho.csv' in item.lower():
                    self.df_desenho = pd.read_csv(
                        os.path.join(source_dir, item),
                        encoding = 'ANSI',
                        sep=';', 
                        index_col=False,
                        header=0,
                        usecols=[' Nº LM', ' Tag/Código', ' Quantidade em BOM', ' Descrição', ' Peso Unit']
                    )
                if 'distribuicao.csv' in item.lower():
                    self.df_distribuicao = pd.read_csv(
                        os.path.join(source_dir, item),
                        encoding = 'ANSI',
                        sep=';', 
                        index_col=False,
                        header=0,
                        usecols=[' DOC REF (ITEM) ', ' CONTRATADA ', ' RESERVA/RODADA ', ' TAG ', ' QT SOLICITADA ', ' QT ENTREGUE ', ' COMENTÁRIOS ']
                    )
                

    def get_status_desenho(self):
        return self.__class__._clean_bom(self.df_desenho)

    def get_recebimento(self):
        return self.__class__._clean_recebimento(self.df_recebimento)

    def get_distribuicao(self):
        return self.__class__._clean_distribuicao(self.df_distribuicao)


    @staticmethod
    def _clean_bom(df):
        df = df.rename(columns={
            ' Tag/Código': 'tag', 
            ' Quantidade em BOM': 'qtd_desenho',
            ' Descrição': 'descricao',
            ' Peso Unit': 'peso_un'
        })
        df['tag'] = df['tag'].str.replace(' ', '')
        df['tag'] = df['tag'].str.upper()
        df['cwp'] = df[' Nº LM'].str.replace(' ', '').str.split('/').str[-1]
        df['peso_un'] = df['peso_un'].astype(float)
        df['cwp_number'] = df['cwp'].str.split('-').str[-1]
        df['chave'] = df['cwp_number'].str.zfill(3) + '-' + df['tag']
        df = df.drop(columns=[' Nº LM'])
        df = df.drop_duplicates(subset=['cwp', 'tag'])

        df.loc[df['tag'] == '93644A', 'peso_un'] = 66.7
        df.loc[df['tag'] == '1033076', 'peso_un'] = 0.083
        df.loc[df['tag'] == 'TR-03-TR-68-0045-03-68-0045G', 'peso_un'] = 214.945
        df.loc[df['tag'] == 'TR-03-TR-68-0090-03-68-0090AE', 'peso_un'] = 124.805
        df.loc[df['tag'] == 'TR-03-TR-68-0090-03-68-0090BY', 'peso_un'] = 2.91375

        return df

    @staticmethod
    def _clean_recebimento(df):
        df = df.rename(columns={' TAG/CÓDIGO ': 'tag', ' QT RECEBIDA ': 'qtd_recebida', ' FORNECEDOR ': 'fornecedor'})
        df['tag'] = df['tag'].str.upper().str.replace(' ', '')
        df['qtd_recebida'] = df['qtd_recebida'].astype(float)
        df = df.groupby('tag', as_index=False).sum()
        return df

    @staticmethod
    def _clean_distribuicao(df):
        df = df.rename(columns={
            ' DOC REF (ITEM) ': 'cwp',
            ' CONTRATADA ': 'contratada', 
            ' RESERVA/RODADA ': 'iwp',
            ' TAG ': 'tag',
            ' QT SOLICITADA ': 'qtd_solicitada', 
            ' QT ENTREGUE ': 'qtd_entregue',
            ' COMENTÁRIOS ': 'comentarios'
        })
        df['iwp'] = df['iwp']\
            .str.split('/').str[0]\
            .str.split('CWP.').str[-1]
        
        df['cwp'] = df['cwp'].str.replace(' ', '')
        df['tag'] = df['tag'].str.replace(' ', '')
        df['qtd_entregue'] = df['qtd_entregue'].str.replace(' ', '')
        df.loc[df['qtd_entregue'] == '', 'qtd_entregue'] = 0
        df.loc[df['qtd_solicitada'] == '', 'qtd_solicitada'] = 0
        df['qtd_entregue'] = df['qtd_entregue'].astype(float)
        df['qtd_solicitada'] = df['qtd_solicitada'].astype(float)
        df[['qtd_solicitada', 'qtd_entregue']] = df[['qtd_solicitada', 'qtd_entregue']].fillna(0) 
        df.loc[df['iwp'].str.len() != 3, 'iwp'] = None
        df = df.dropna(subset=['tag'])           
        return df

    @staticmethod
    def _get_quantities(df, df_warehouse):   
        df['qtd_recebida'] = 0
        df_proxy = df.loc[df['tag'].isin(df_warehouse['tag'])].copy()
        for idx, row in df_proxy.iterrows():
            qtd_recebida = df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida']
            qtd_faltante = row['qtd_desenho'] - row['qtd_recebida']
            if qtd_faltante > 0 and not qtd_recebida.empty:
                qtd_recebida = qtd_recebida.iloc[0]
                if qtd_recebida >= qtd_faltante:
                    df.loc[idx, 'qtd_recebida'] = qtd_faltante
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida'] = qtd_recebida - qtd_faltante
                else:
                    df.loc[idx, 'qtd_recebida'] = qtd_recebida
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida'] = 0
        return df
