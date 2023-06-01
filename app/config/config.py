import os

class Ifc():
    config = {
        'FAM':{
            'geometry_settings': False,
            'params': {
                'tag': ('Tekla Assembly', 'Assembly/Cast unit Mark'),
                'cwp': ('Default', 'USER_FIELD_1'),
            }
        },
        'EMALTO':{
            'geometry_settings': False,
            'params': {
                'tag': ('Tekla Assembly', 'ASSEMBLY_POS'),
                'cwp': ('Tekla Assembly', 'USER_FIELD_1'),
                'fabricante': ('Tekla Assembly', 'USER_FIELD_2'),
            }
        },
        'CODEME':{
            'geometry_settings': False,
            'params': {
                'tag': ('Default', 'USER_FIELD_3'),
                'cwp': ('Default', 'USER_FIELD_4'),
            }
        },
        'SINOSTEEL':{
            'geometry_settings': True,
            'params': {
                'tag': ('Tekla Assembly', 'Assembly/Cast unit Mark'),
                'cwp': ('Default', 'USER_FIELD_1'),
            }
        }
    }


class Project():
    REPOSITORY_BRANCH = r'\BI\02. Repositório de Arquivos'
    DASHBOARD_BRANCH = r'\BI\01. Dashboards Ativos'

    def __init__(self, name:str, PROJECT_BRANCH:str) -> None:
        self.name = name
        self.PROJECT_PATH = os.environ['ROOT_PATH'] + PROJECT_BRANCH
        self.REPOSITORY_PATH = self.PROJECT_PATH + self.__class__.REPOSITORY_BRANCH
        self.DASHBOARD_PATH = self.PROJECT_PATH + self.__class__.DASHBOARD_BRANCH

        self.MEMORIA_CALCULO_PATH = os.path.dirname(os.environ['ROOT_PATH']) + r'\02.Prazo\Proj - Capanema\_Old\02. Cronograma AWP\Memória de Cálculo'
        self.REPORTS_PATH = self.PROJECT_PATH + r'\SMAT\REPORT'
        self.SUMMARY_PATH = self.REPOSITORY_PATH+ r'\Cronogramas\Summary'
        self.LX_PATH = self.PROJECT_PATH + r'\SMAT\LX'
        self.LX_REPOSITORY_PATH = self.REPOSITORY_PATH + r'\LX\LX Geral'
        self.MAPPER_PATH = self.REPOSITORY_PATH + r'\LX'
        self.MASTERPLAN_PATH = self.REPOSITORY_PATH + r'\Cronogramas\Masterplan'
        self.FORNECEDORES_PATH = self.REPOSITORY_PATH + r'\Cronogramas\Fornecedores'
        self.MONTADORA_PATH = self.REPOSITORY_PATH + r'\Cronogramas\Montadora'
        self.TRACER_PATH = self.REPOSITORY_PATH + r'\Modelos BIM\Stagging'
        self.PRODUCAO_PATH = self.REPOSITORY_PATH + r'\Status de Producao'
        self.ROMANEIO_PATH = self.REPOSITORY_PATH + r'\Romaneio'
        self.PQ_PATH = self.REPOSITORY_PATH + r'\PQ'
        self.IFC_PATH = self.REPOSITORY_PATH + r'\Modelos BIM\IFC'
        self.DB_PATH = self.REPOSITORY_PATH + r'\Modelos BIM\DB Tracer'
        self.STAGGING_PATH = self.REPOSITORY_PATH + r'\Modelos BIM\Stagging'
        self.FEDERATED_PATH = self.REPOSITORY_PATH + r'\Modelos BIM\IFC com status'
        self.VCAD_PATH = self.REPOSITORY_PATH + r'\VCad'

        self.OUTPUT_FAM = self.DASHBOARD_PATH + r'\Fornecimento\FAM Steel'
        self.OUTPUT_EMALTO = self.DASHBOARD_PATH + r'\Fornecimento\Emalto'
        self.OUTPUT_CODEME = self.DASHBOARD_PATH + r'\Fornecimento\Codeme'
        self.OUTPUT_SINOSTEEL = self.DASHBOARD_PATH + r'\Fornecimento\Sinosteel'
        self.OUTPUT_MONTAGEM_ELETROMECANICA = self.DASHBOARD_PATH + r'\Montagem Eletromecanica'
        self.OUTPUT_GESTAO_MATERIAIS = self.DASHBOARD_PATH + r'\Gestão de Materiais'
        self.OUTPUT_ML_LX = self.DASHBOARD_PATH + r'\Checagem de LX'
        self.OUTPUT_FEDERADO = self.DASHBOARD_PATH + r'\Modelo Federado'
        

    def __repr__(self) -> str:
        return self.name

    def dump(self):
        for key, value in self.__dict__.items():
            os.environ[f'{key}_{self.name}'] = value

    
def run_config():
    newsteel = Project('NEWSTEEL', r'\Proj - New Steel')
    newsteel.dump()

    capanema = Project('CAPANEMA', r'\Proj - Capanema')
    capanema.dump()

