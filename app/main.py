from dotenv import load_dotenv
from config.config import run_config
load_dotenv('.env')
run_config()

from pipelines import gestao_materiais, montagem_eletromecanica, bim_x_pwp



# lx_check.run_pipeline(
#     reports_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\SMAT\REPORT', 
#     lx_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\LX\LX Geral', 
#     mapper_file=r"C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\LX\LX-FORNECEDORES-NEW-STEEL.xlsx", 
#     tracer_data_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\Modelos BIM\Staging Files',
#     output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\01. Dashboards Ativos\Checagem de LX'
# )


# montagem_eletromecanica.newsteel()
# gestao_materiais.newsteel()
# bim_x_pwp.famsteel()


montagem_eletromecanica.capanema()
gestao_materiais.capanema()
bim_x_pwp.codeme()
bim_x_pwp.sinosteel()