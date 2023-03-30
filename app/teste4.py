import pandas as pd
import os
from pipelines import extract_ifc_data
from dotenv import load_dotenv
from config.config import run_config
load_dotenv('.env')
run_config()

pd.set_option('display.max_rows', 200)
# f1 = r"C:\Users\emman\Downloads\069-002.csv"
# f2 = r"C:\Users\emman\Downloads\068-002.csv"

# dfs = []
# for file_path in [f1, f2]:
#     df = pd.read_csv(file_path, sep=';', usecols=['Tekla Assembly\r\r\nGLOBALID',], index_col=False).rename(columns={'Tekla Assembly\r\r\nGLOBALID':'guid'})
#     df['file_name'] = os.path.abspath(file_path)
#     dfs.append(df)

# df = pd.concat(dfs).dropna(subset=['guid'])
if __name__ == "__main__":
    extract_ifc_data.sinosteel(
        input_ifc_folder=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\IFC Editados', 
        input_db_folder=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\DB Tracer', 
        output_folder=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\Stagging', 
        use_files=None
    )

