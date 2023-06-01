from config.config import run_config
from dotenv import load_dotenv

load_dotenv('.env')
run_config()

from pipelines import (bim_x_pwp, extract_ifc_data, federated_model, gestao_materiais, lx_check, montagem_eletromecanica)

if __name__ == "__main__":
    # extract_ifc_data.sinosteel(use_files='CF-S1985-006-M-MT-CWP-069-TR-1220CF-02')
    # extract_ifc_data.codeme(use_files=)
    # extract_ifc_data.emalto()
    # extract_ifc_data.famsteel()

    # lx_check.newsteel()
    # montagem_eletromecanica.newsteel()
    # gestao_materiais.newsteel()
    # bim_x_pwp.emalto()
    # bim_x_pwp.famsteel()

    # montagem_eletromecanica.capanema()
    # gestao_materiais.capanema()
    # bim_x_pwp.codeme()
    bim_x_pwp.sinosteel()
    # federated_model.capanema()  
    # federated_model.vcad()      