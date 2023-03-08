import os
import sqlite3
import pandas as pd
import ifcopenshell
import ifcopenshell.util.placement

class TracerFullReport():
    def __init__(self, source_dir=None) -> None:
        self.source_dir = source_dir

    def get_report(self):
        self._read_staging_data()
        return self.df_report

    def _read_staging_data(self, source_dir=None):
        self.source_dir = source_dir if source_dir else self.source_dir
        df_list = [pd.read_parquet(os.path.join(self.source_dir, item)) for item in os.listdir(self.source_dir)]
        self.df_report = pd.concat(df_list, ignore_index=True)


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



            