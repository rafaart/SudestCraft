import os
import ifcopenshell
import ifcopenshell.geom
import pandas as pd
import dill
from config.config import Ifc
from suppliers import SuppliersLX
from bim_tools import IfcDataBase, IfcElement, IfcDataExplorer, extract_params
import numpy as np
import multiprocessing as mp
from multiprocessing import shared_memory
import time
import sys


config = Ifc.config
num_workers = mp.cpu_count()  

def _get_colors(geometry_settings, element):
    try:
        shape = ifcopenshell.geom.create_shape(geometry_settings, element)
        [material] = shape.geometry.materials
        color = material.diffuse
        color_r = color[0]
        color_g = color[1]
        color_b = color[2]
    except:
        color_r = color_g = color_b = None
    return {'color_r': color_r, 'color_g': color_g, 'color_b': color_b}


def _params_by_id(row, ifc):
    guid = row['IfcId']
    config = row['config']
    geometry_settings = config['geometry_settings']
    config = config['params']
    element = ifc.by_guid(guid)
    params = {'IfcId': guid}
    params['name'] = element.Name
    location = ifcopenshell.util.placement.get_local_placement(element.ObjectPlacement)
    params['location_x'] = location[0][-1]
    params['location_y'] = location[1][-1]
    params['location_z'] = location[2][-1]
    if geometry_settings:
        color_params = _get_colors(geometry_settings, element)
        params.update(color_params)
    try:
        assembly = ifcopenshell.util.element.get_aggregate(element) #obter assembly    
        ifc_psets = ifcopenshell.util.element.get_psets(element=assembly)
        params['assembly_id'] = assembly.get_info()['GlobalId']
        for param, (pset, mapped_param) in config.items():
            params[param] = ifc_psets[pset][mapped_param]
    except:
        params['assembly_id'] = element.get_info()['GlobalId']
        for param, (pset, mapped_param) in config.items():
            params[param] = None
    return params
   


def _process_data_chunk(ifc_path, df_data_chunk):
    ifc = ifcopenshell.open(ifc_path)
    params = list(df_data_chunk.apply(_params_by_id, ifc=ifc, axis=1))
    return pd.DataFrame(params)



def codeme(input_ifc_folder, input_db_folder, output_folder, use_files=None):
    lx_capanema_dir = SuppliersLX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
    df_lx = lx_capanema_dir.get_report()
    df_lx['ifc_file_name'] =  df_lx['cwp'] + '-' + df_lx['cod_ativo']
    df_lx = df_lx[['ifc_file_name', 'supplier']].drop_duplicates(subset='ifc_file_name', keep='first')
    
    files_names = [item for item in os.listdir(input_db_folder) if os.path.isfile(os.path.join(input_db_folder, item))]
    if use_files:
        use_files = use_files if isinstance(use_files, list) else [use_files]
        files_names = [file for file in files_names if file.split('.')[0] in use_files]
    files_names = [file_name for file_name in files_names if file_name.split('.')[0] in df_lx['ifc_file_name'].to_list()]

    for file_name in files_names:
        db_file_path = os.path.join(input_db_folder, file_name)
        ifc_file_path = os.path.join(input_ifc_folder, file_name.replace('.db', '.ifc'))
        destination_file_path = os.path.join(output_folder, file_name.replace('.db', '.parquet'))
        
        ifc_data = IfcDataBase(db_file_path)
        df_elements = ifc_data.Element
        df_elements =  df_elements.loc[df_elements['Mesh'].str.len() > 30, ['IfcId', 'Mesh']]
        df_elements['ifc_file_name'] = os.path.basename(file_name).replace('.db', '')

        df_elements = pd.merge(
            left= df_elements,
            right= df_lx[['ifc_file_name', 'supplier']],
            on='ifc_file_name',
            how='left'
        )

        df_elements['config'] = df_elements['supplier'].apply(lambda suppl: config[suppl])
        data_chunks = np.array_split(df_elements[['IfcId', 'config']], num_workers)

        with mp.Pool(processes=num_workers) as pool:
            pool_results = [pool.apply_async(_process_data_chunk, (ifc_file_path, chunk)) for chunk in data_chunks]
            results = [res.get() for res in pool_results]
        
        df_params = pd.concat(results)
        df_main = pd.merge(
            left=df_elements,
            right=df_params,
            on='IfcId',
            how='left'
        )
        df_main.to_parquet(destination_file_path, index=False)






