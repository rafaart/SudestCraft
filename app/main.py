from dotenv import load_dotenv
from config.config import run_config
load_dotenv('.env')
run_config()

from pipelines import gestao_materiais, montagem_eletromecanica, bim_x_pwp, lx_check



# lx_check.newsteel()

# montagem_eletromecanica.newsteel()
# gestao_materiais.newsteel()
# bim_x_pwp.famsteel()

# montagem_eletromecanica.capanema()
# gestao_materiais.capanema()
# bim_x_pwp.codeme()
bim_x_pwp.sinosteel()