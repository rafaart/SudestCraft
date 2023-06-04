import pandas as pd
import os

    
class Summary:
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                if all(key in item.lower() for key in ['summary']):
                    self.df_workbook = pd.read_excel(
                        os.path.join(source_dir, item),
                        usecols=[
                            'IWP Name', 
                            'IWP Description', 
                            'IWP Discipline', 
                            'IWP Planned Start Date', 
                            'IWP Planned Finish Date', 
                            'IWP Purpose',
                            'HxGNBR_StringAttribute2',
                            'Component UID'
                        ]
                    )
                    break
    
    def get_report(self):
        return self.__class__._clean_report(self.df_workbook)


    @staticmethod
    def _clean_report(df):
        df = df.rename(columns={
            'IWP Name': 'iwp', 
            'IWP Description': 'descricao', 
            'IWP Discipline': 'disciplina',
            'IWP Planned Start Date': 'data_inicio', 
            'IWP Planned Finish Date': 'data_termino', 
            'IWP Purpose': 'proposito',
            'HxGNBR_StringAttribute2': 'tag',
            'Component UID': 'guid' 
        })
        df = df.loc[df['proposito'].str.contains('Montagem', na=False)]
        df= df.sort_values(by=['data_inicio', 'proposito'], ascending=[True, False])
        df = df.dropna(subset='tag')
        df = df.drop_duplicates(subset=['guid', 'tag'], keep='first')     
               
        df['cwp'] = df['iwp'].str.split('.').str[:-1].str.join('.')
        df['cwa'] = df['iwp'].str.split('-').str[2]     

        df_numerical = df[['iwp', 'tag']].groupby(['iwp', 'tag'], as_index=False).size().rename(columns={'size':'qtd_summary'})
        df_description = df.drop_duplicates(subset=['iwp', 'tag'], keep='first')

        df = pd.merge(
            left=df_numerical,
            right=df_description,
            on=['iwp', 'tag'],
            how='inner'
        )
        return df
    