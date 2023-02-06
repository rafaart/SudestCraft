from dotenv import load_dotenv
from config import run_config
load_dotenv('.env')
run_config()

import pipeline
from pipelines import lx_check


# lx_check.run_pipeline(
#     reports_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\SMAT\REPORT', 
#     lx_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\LX\LX Geral', 
#     mapper_file=r"C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\LX\LX-FORNECEDORES-NEW-STEEL.xlsx", 
#     tracer_data_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\Modelos BIM\Staging Files',
#     output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\01. Dashboards Ativos\Checagem de LX'
# )


# pipeline.montagem_eletromecanica_newsteel(
#     output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\01. Dashboards Ativos\Montagem Eletromecanica'
# )

pipeline.fornecimento_new_steel_fam( 
    output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\01. Dashboards Ativos\Fornecimento'
)

# pipeline.gestao_materiais_newsteel(
#     output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\01. Dashboards Ativos\Gestão de Materiais'
# )

# pipeline.montagem_eletromecanica_capanema(
#     output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\01. Dashboards Ativos\Montagem Eletromecanica'
# )

# pipeline.fornecimento_capanema_codeme(
#     output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\01. Dashboards Ativos\Fornecimento\Codeme'
# )

# pipeline.fornecimento_capanema_sinosteel(
#     output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\01. Dashboards Ativos\Fornecimento\Sinosteel'
# )

# pipeline.gestao_materiais_capanema(
#     output_dir=r'C:\Users\EmmanuelSantana\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\01. Dashboards Ativos\Gestão de Materiais'
# )

