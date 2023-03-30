import os
import sqlite3
import pandas as pd
import ifcopenshell
import ifcopenshell.util.placement

        
class TracerFullReport():
    def __init__(self, source_dir=None) -> None:
        self.source_dir = source_dir

    def get_report(self):
        self._apply_order()
        return self.df_report

    def read_stagging_data(self, source_dir=None):
        self.source_dir = source_dir if source_dir else self.source_dir
        df_list = [pd.read_parquet(os.path.join(self.source_dir, item)) for item in os.listdir(self.source_dir)]
        self.df_raw_report = pd.concat(df_list, ignore_index=True)
        return self

    def drop_missplaced_elements(self):
        self.df_raw_report.loc[ self.df_raw_report['cwp'].isna(), 'cwp'] =  self.df_raw_report['file_name'].str[:25]
        return self


    def _apply_order(self):
        df = self.df_raw_report
        df = df.drop(columns=['order'])
        df['location_z'] = df['location_z'].astype(float)
        df = df.sort_values(by=['location_z', 'location_x'], ascending=[True, False])
        df_order = df.dropna(subset=['location_z', 'location_x'])
        df_order = df_order.drop_duplicates(subset=['agg_id'], keep='first')
        df_order['order']  = df_order.groupby(['cwp', 'tag'], as_index=False).cumcount()
        df_order['order'] = df_order['order'] + 1
        df = pd.merge(
            df,
            df_order[['agg_id', 'order']],
            on='agg_id',
            how='left'
        )
        self.df_report = df


class IfcDataBase():
    def __init__(self, connection_string) -> None:
        self.tables = [
            'Element', 
            'ElementParameterText', 
            'ElementParameterInteger', 
            'ElementParameterNumber', 
            'Document'
        ]
        self.cnx = sqlite3.connect(connection_string)
        self._get_data()

    def _get_data(self):
        for table in self.tables:
            self.__dict__[table] = pd.read_sql_query(f"SELECT * FROM {table}", self.cnx)



            