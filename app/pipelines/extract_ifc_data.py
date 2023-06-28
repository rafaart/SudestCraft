import os
import ifcopenshell
import ifcopenshell.geom
import pandas as pd
from config.config import Ifc
from data_sources.LX import LX
from data_sources.ifc_sources import IfcDataBase
import numpy as np
import multiprocessing as mp
import json


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


def _params_by_id(row, ifc, geometry_settings):
    guid = row['IfcId']
    config = json.loads(row['config'])
    geometry_settings = geometry_settings if config['geometry_settings'] else None
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
        assembly = ifcopenshell.util.element.get_aggregate(element)
        ifc_psets = ifcopenshell.util.element.get_psets(element=assembly)
        params['agg_id'] = assembly.get_info()['GlobalId']
        for param, (pset, mapped_param) in config.items():
            params[param] = ifc_psets[pset][mapped_param]
    except:
        ifc_psets = ifcopenshell.util.element.get_psets(element=element)
        params['agg_id'] = element.get_info()['GlobalId']
        for param, (pset, mapped_param) in config.items():
            try:
                params[param] = ifc_psets[pset][mapped_param]
            except:
                params[param] = None
    return params
   


def _process_data_chunk(ifc_path, df_data_chunk):
    ifc = ifcopenshell.open(ifc_path)
    geometry_settings = ifcopenshell.geom.settings()
    params = list(df_data_chunk.apply(_params_by_id, ifc=ifc, geometry_settings=geometry_settings, axis=1))
    return pd.DataFrame(params)



def codeme(use_files=None):
    input_db_folder = os.environ['DB_PATH_CAPANEMA']
    input_ifc_folder = os.environ['IFC_PATH_CAPANEMA']
    output_folder = os.environ['STAGGING_PATH_CAPANEMA']

    print('Number of workers: ', num_workers)
    lx_capanema_dir = SuppliersLX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
    df_lx = lx_capanema_dir.get_report()
    df_lx = df_lx.loc[df_lx['supplier'] == 'CODEME']
    
    df_lx = df_lx[['cwp', 'supplier']].drop_duplicates(subset='cwp', keep='first')
    files_names = os.listdir(input_db_folder)
    if use_files:
        files_names = use_files if isinstance(use_files, list) else [use_files]

    files_names = [name for name in files_names if name[0:25] in df_lx['cwp'].to_list()]
    for file_name in files_names:
        print('Processing file: ', file_name.split('.')[0])
        db_file_path = os.path.join(input_db_folder, file_name)
        ifc_file_path = os.path.join(input_ifc_folder, file_name.replace('.db', '.ifc'))
        destination_file_path = os.path.join(output_folder, file_name.replace('.db', '.parquet'))
        
        ifc_data = IfcDataBase(db_file_path)
        df_elements = ifc_data.Element
        df_elements =  df_elements.loc[df_elements['Mesh'].str.len() > 30, ['IfcId', 'Mesh']]
        df_elements['file_name'] = os.path.basename(file_name).replace('.db', '')
        df_elements['supplier'] = 'CODEME'
        df_elements['config'] = json.dumps(config['CODEME'])
        data_chunks = np.array_split(df_elements[['IfcId', 'config']], num_workers / 2)

        with mp.Pool(processes=num_workers) as pool:
            pool_results = [pool.apply_async(_process_data_chunk, args=(ifc_file_path, chunk)) for chunk in data_chunks]
            results = [res.get() for res in pool_results]
        
        df_params = pd.concat(results)
        df_main = pd.merge(
            left=df_elements.drop(columns=['config']),
            right=df_params,
            on='IfcId',
            how='left'
        )
        print("Saving file...")
        df_main.to_parquet(destination_file_path, index=False)



def sinosteel(use_files=None):
    input_db_folder = os.environ['DB_PATH_CAPANEMA']
    input_ifc_folder = os.environ['IFC_PATH_CAPANEMA']
    output_folder = os.environ['STAGGING_PATH_CAPANEMA']

    print('Number of workers: ', num_workers)
    lx_capanema_dir = SuppliersLX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
    df_lx = lx_capanema_dir.get_report()
    df_lx = df_lx.loc[df_lx['supplier'] == 'SINOSTEEL']
    df_lx = df_lx[['cwp', 'supplier']].drop_duplicates(subset='cwp', keep='first')
    files_names = os.listdir(input_db_folder)
    if use_files:
        files_names = use_files if isinstance(use_files, list) else [use_files]

    files_names = [name for name in files_names if name[0:25] in df_lx['cwp'].to_list()]
    for file_name in files_names:
        print('Processing file: ', file_name.split('.')[0])
        db_file_path = os.path.join(input_db_folder, file_name)
        ifc_file_path = os.path.join(input_ifc_folder, file_name.replace('.db', '.ifc'))
        destination_file_path = os.path.join(output_folder, file_name.replace('.db', '.parquet'))
        
        ifc_data = IfcDataBase(db_file_path)
        df_elements = ifc_data.Element
        df_elements =  df_elements.loc[df_elements['Mesh'].str.len() > 30, ['IfcId', 'Mesh']]
        df_elements['file_name'] = os.path.basename(file_name).replace('.db', '')
        df_elements['supplier'] = 'SINOSTEEL'
        df_elements['config'] = json.dumps(config['SINOSTEEL'])
        data_chunks = np.array_split(df_elements[['IfcId', 'config']], num_workers / 2)

        with mp.Pool(processes=num_workers) as pool:
            pool_results = [pool.apply_async(_process_data_chunk, args=(ifc_file_path, chunk)) for chunk in data_chunks]
            results = [res.get() for res in pool_results]
        
        df_params = pd.concat(results)
        df_main = pd.merge(
            left=df_elements.drop(columns=['config']),
            right=df_params,
            on='IfcId',
            how='left'
        )
        print("Saving file...")
        df_main.to_parquet(destination_file_path, index=False)


def emalto(use_files=None):
    print('Number of workers: ', num_workers)
    input_db_folder = os.environ['DB_PATH_NEWSTEEL']
    input_ifc_folder = os.environ['IFC_PATH_NEWSTEEL']
    output_folder = os.environ['STAGGING_PATH_NEWSTEEL']

    files_names = os.listdir(input_db_folder)
    if use_files:
        files_names = use_files if isinstance(use_files, list) else [use_files]

    files_names = [name for name in files_names if 'EMALTO' in name]
    for file_name in files_names:
        print('Processing file: ', file_name.split('.')[0])
        db_file_path = os.path.join(input_db_folder, file_name)
        ifc_file_path = os.path.join(input_ifc_folder, file_name.replace('.db', '.ifc'))
        destination_file_path = os.path.join(output_folder, file_name.replace('.db', '.parquet'))
        
        ifc_data = IfcDataBase(db_file_path)
        df_elements = ifc_data.Element
        df_elements =  df_elements.loc[df_elements['Mesh'].str.len() > 30, ['IfcId', 'Mesh']]
        df_elements['file_name'] = os.path.basename(file_name).replace('.db', '').replace('_EMALTO', '') 
        df_elements['supplier'] = 'EMALTO'
        df_elements['config'] = json.dumps(config['EMALTO'])
        data_chunks = np.array_split(df_elements[['IfcId', 'config']], num_workers / 2)

        with mp.Pool(processes=num_workers) as pool:
            pool_results = [pool.apply_async(_process_data_chunk, args=(ifc_file_path, chunk)) for chunk in data_chunks]
            results = [res.get() for res in pool_results]
        
        df_params = pd.concat(results)
        df_main = pd.merge(
            left=df_elements.drop(columns=['config']),
            right=df_params,
            on='IfcId',
            how='left'
        )
        print("Saving file...")
        df_main.to_parquet(destination_file_path, index=False)


def famsteel(use_files=None):
    input_db_folder = os.environ['DB_PATH_NEWSTEEL']
    input_ifc_folder = os.environ['IFC_PATH_NEWSTEEL']
    output_folder = os.environ['STAGGING_PATH_NEWSTEEL']

    print('Number of workers: ', num_workers)
    files_names = os.listdir(input_db_folder)
    if use_files:
        use_files = use_files if isinstance(use_files, list) else [use_files]
        files_names = [file for file in files_names if file.split('.')[0] in use_files]

    files_names = [name for name in files_names if 'FAM' in name]
    for file_name in files_names:
        print('Processing file: ', file_name.split('.')[0])
        db_file_path = os.path.join(input_db_folder, file_name)
        ifc_file_path = os.path.join(input_ifc_folder, file_name.replace('.db', '.ifc'))
        destination_file_path = os.path.join(output_folder, file_name.replace('.db', '.parquet'))
        
        ifc_data = IfcDataBase(db_file_path)
        df_elements = ifc_data.Element
        df_elements =  df_elements.loc[df_elements['Mesh'].str.len() > 30, ['IfcId', 'Mesh']]
        df_elements['file_name'] = os.path.basename(file_name).replace('.db', '').replace('_EMALTO', '') 
        df_elements['supplier'] = 'FAM'
        df_elements['config'] = json.dumps(config['FAM'])
        data_chunks = np.array_split(df_elements[['IfcId', 'config']], num_workers / 2)

        with mp.Pool(processes=num_workers) as pool:
            pool_results = [pool.apply_async(_process_data_chunk, args=(ifc_file_path, chunk)) for chunk in data_chunks]
            results = [res.get() for res in pool_results]
        
        df_params = pd.concat(results)
        df_main = pd.merge(
            left=df_elements.drop(columns=['config']),
            right=df_params,
            on='IfcId',
            how='left'
        )
        print("Saving file...")
        df_main.to_parquet(destination_file_path, index=False)