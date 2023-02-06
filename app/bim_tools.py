import os
import pandas as pd

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
