from dotenv import load_dotenv
from config.config import run_config
load_dotenv('.env')
run_config()

from pipelines import gestao_materiais, montagem_eletromecanica, bim_x_pwp, lx_check, extract_ifc_data, federated_model


if __name__ == "__main__":
    extract_ifc_data.sinosteel()
    # extract_ifc_data.codeme()
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
    # bim_x_pwp.sinosteel()
    # federated_model.capanema()  
    # federated_model.vcad_model() 
    # federated_model.vcad()      