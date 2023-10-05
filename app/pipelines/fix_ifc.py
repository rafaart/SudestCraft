import os
import ifcopenshell
import ifcopenshell.geom
import ifcopenshell.util.shape
from data_sources.ifc_sources import IfcDataBase


fix_mapper = {
    'CF-S1985-006-M-MT-CWP-069-TR-1220CF-01':[
        '1ab3ZZ000GMZ4sE3SqCpKr',
        '1ab3ZZ000FV34sE3SqCpKr',
        '1ab3ZZ000GEZ4sE3SqCpKr',
        '1ab3ZZ000Fz34sE3SqCpKr',
        '1ab3ZZ000Fpp4sE3SqCpKr',
        '1ab3ZZ000Go34sE3SqCpKr',
        '1ab3ZZ000FgZ4sE3SqCpKr'
    ],
}

mapper_by_type_and_description = {
    'CF-S1985-006-M-MT-CWP-069-TR-1220CF-01':
        {
            'IFCBEAM':[
                'TELA1057X3',
                ],
        },
    'CF-S1985-008-M-MT-CWP-094-TR-1220CF-03':
        {
            'IFCDISCRETEACCESSORY':[
                'TELA3.4X727.6', 
                'TELA3.4X730.4',
                'TELA3.4X766.5',
                'TELA1058X3.4',
                'TELA3.4X1050.4',
                'TELA1018X3.4',
                'TELA3.4X752.7'
            ],
            'IFCBEAM':
            [
                'TELA1057X3', 
                'TELA1018X3',
                'TELA1058X3',
                'TELA1057X3'
            ],
            'IFCPLATE':
            [
                'TELA3X728',
                'TELA3X1030',
                'TELA3X730',
                'TELA3X766',
                'TELA3X1345',
                'TELA2X250'
            ]
        },
    'CF-S1985-008-M-MT-CWP-096-TR-1220CF-05':
        {
            'IFCDISCRETEACCESSORY':[
                'TELA3.4X727.6',
                'TELA1018X3.4'
            ],

            'IFCBEAM':
            [
            'TELA1057X3',
            'TELA1018X3'
            ],

            'IFCPLATE':
            [
            'TELA3X728',
            'TELA3X1075'
            ]
        },
    'CF-S1985-008-M-MT-CWP-097-TR-1220CF-07':
    {
        'IFCDISCRETEACCESSORY':
        [
            'TELA3.4X730.4',
            'TELA3.4X752.7'
        ]
    },
    'CF-S1985-008-M-MT-CWP-738-AL-1360CF-01':
        {
            'IFCDISCRETEACCESSORY':
            [
                'W 200 X 31.3'
            ]
        },
    'CF-S1985-008-M-MT-CWP-738-TR-1360CF-09':
        {
            'IFCDISCRETEACCESSORY':
            [
                'CH2X880'
            ]
        },
    'CF-S1985-008-M-MT-CWP-739-CT-1360CF-12':
        {
            'IFCDISCRETEACCESSORY':
            [
                'TELA3.4X1328.2',
                'TELA3.4X730.4'
            ],
            'IFCBEAM':
            [
                'TELA1057X3',
                'TELA257X3',
                'TELA877X3',
                'TELA930X3',
                'TELA857X3'  
            ],
            'IFCPLATE':
            [
                'TELA3X730',
                'TELA3X779'
            ]
        },
    'CF-S1985-008-M-MT-CWP-739-TR-1360CF-11':
        {
            'IFCDISCRETEACCESSORY':
            [
                'CH2X880',
                'TELA1037X3.4',
                'TELA1400X3.4',
            ],
            'IFCBEAM':
            [
                'TELA1018X3.4',
                'TELA1037X3',
                'TELA1057X3',
                'TELA1400X3',
                'TELA1018X3' 
            ]
        },
    'CF-S1985-008-M-MT-CWP-739-TR-1360CF-12':
        {
            'IFCDISCRETEACCESSORY':
            [
                'TELA3.4X1328.2'
            ],
            'IFCBEAM':
            [
                'TELA1057X3'
            ],
        },
    'CF-S1985-008-M-MT-CWP-830-TR-1220CF-10':
        {
            'IFCPLATE':
            [
                'CH80X167'
            ],
        },
    'CF-S1985-009-M-MT-CWP-113-TR-1220CF-06':
        {
            'IFCDISCRETEACCESSORY':
            [
                'TELA1056.7X3.4'
            ],
        },
        'CF-S1985-001-M-MT-CWP-015_TR-1210CF-01_REV_G':
        {
            'IFCBEAM':
            [
                'TELA1057X3'
            ],
            'IFCPLATE':
            [
                'TELA3X730'
            ],
        },
}


# for ifc_id in mapper[basename]:
#     try:
#         element = ifc.by_guid(ifc_id)
#         ifc.remove(element)
#     except:
#         print(f'IfcId not found: {ifc_id}')


def _delete_elements(files, mapper):
    for file_path in files:
        basename = os.path.basename(file_path).replace('.ifc', '')
        if basename in mapper.keys():
            print(f'Processing file: {basename}')
            ifc = ifcopenshell.open(file_path)
            for ifc_type in mapper[basename].keys():
                elements = ifc.by_type(ifc_type)
                for element in elements:
                    if element.Description in mapper[basename][ifc_type]:
                        try:
                            ifc.remove(element)
                        except:
                            print(f'IfcId not found: {element.Id}')
            ifc.write(file_path)
        else:
            print(f'No items were removed from: {basename}')

def capanema(by="type"):
    input_ifc_folder = os.environ['IFC_PATH_CAPANEMA']
    files_paths = [os.path.join(input_ifc_folder, file_name) for file_name in os.listdir(input_ifc_folder)]
    if by == "type":
        _delete_elements(files_paths, mapper_by_type_and_description)
    if by == "elements":
        pass
    # escrever a função opcional para deletar por elementos
    

