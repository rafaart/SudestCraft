import os
import ifcopenshell.geom

class Ifc():
    config = {
        'FAM CONSTRUCOES':{
            'geometry_settings': None,
            'params': {
                'tag': ('Tekla Assembly', 'Assembly/Cast unit Mark'),
                'cwp': ('Default', 'USER_FIELD_1'),
                'position_code': ('Tekla Assembly', 'Assembly/Cast unit position code')
            }
        },
        'CODEME ENGENHARIA':{
            'geometry_settings': None,
            'params': {
                'tag': ('Tekla Assembly', 'Assembly/Cast unit Mark'),
                'cwp': ('Default', 'USER_FIELD_4'),
                'position_code': ('Tekla Assembly', 'Assembly/Cast unit position code')
            }
        },
        'SINOSTEEL':{
            'geometry_settings': ifcopenshell.geom.settings(),
            'params': {
                'tag': ('Tekla Assembly', 'Assembly/Cast unit Mark'),
                'cwp': ('Default', 'USER_FIELD_1'),
                'position_code': ('Tekla Assembly', 'Assembly/Cast unit position code')
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
        self.LX_PATH = self.REPOSITORY_PATH + r'\LX\LX Geral'
        self.MAPPER_PATH = self.REPOSITORY_PATH + r'\LX'
        self.MASTERPLAN_PATH = self.REPOSITORY_PATH + r'\Cronogramas\Masterplan'
        self.FORNECEDORES_PATH = self.REPOSITORY_PATH + r'\Cronogramas\Fornecedores'
        self.MONTADORA_PATH = self.REPOSITORY_PATH + r'\Cronogramas\Montadora'
        self.TRACER_PATH = self.REPOSITORY_PATH + r'\Modelos BIM\Stagging Files'
        self.ROMANEIO_PATH = self.REPOSITORY_PATH + r'\Romaneio FAM Steel'
        self.PQ_PATH = self.REPOSITORY_PATH + r'\PQ'

        self.OUTPUT_FAM = self.DASHBOARD_PATH + r'\Fornecimento\FAM Steel'
        self.OUTPUT_CODEME = self.DASHBOARD_PATH + r'\Fornecimento\Codeme'
        self.OUTPUT_SINOSTEEL = self.DASHBOARD_PATH + r'\Fornecimento\Sinosteel'
        self.OUTPUT_MONTAGEM_ELETROMECANICA = self.DASHBOARD_PATH + r'\Montagem Eletromecanica'
        self.OUTPUT_GESTAO_MATERIAIS = self.DASHBOARD_PATH + r'\Gestão de Materiais'

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

