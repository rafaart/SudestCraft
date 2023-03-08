import pandas as pd

def breakdown_by_axis(df,groupby, axis):
    for cwa_file in df[groupby].drop_duplicates(keep='first'):
        z_median = df.loc[df[groupby] == cwa_file, axis].median()
        df.loc[(df[groupby] == cwa_file) & (df[axis].astype(float) <= z_median), 'part'] = 'Parte 1'
        df.loc[(df[groupby] == cwa_file) & (df[axis].astype(float) > z_median), 'part'] = 'Parte 2'
    return df


def breakdown_by_file_count(df,groupby, min):
    for group in df[groupby].drop_duplicates(keep='first'):
        count = len(df.loc[df[groupby] == group, 'file_name'].drop_duplicates(keep='first'))
        if count >= min:
            for idx, file_name in enumerate(df.loc[df[groupby] == group, 'file_name'].drop_duplicates(keep='first')):
                df.loc[(df[groupby] == group) & (df['file_name'] == file_name), 'part'] = 'Parte ' + str(idx + 1)
    return df



def get_quantities_fam(df, df_warehouse):   
    df['qtd_recebida'] = 0
    for idx, row in df.iterrows():
        row = row.fillna(0)
        qtd_recebida = df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida']
        qtd_desenho = row['qtd_desenho'] if row['qtd_desenho'] else row['qtd_total']
        qtd_entregue = row['qtd_entregue']
        if not qtd_recebida.empty:
            qtd_recebida = qtd_recebida.iloc[0]
            if qtd_recebida >= qtd_desenho:
                df.loc[idx, 'qtd_recebida'] = qtd_desenho
                df.loc[idx, 'qtd_entregue'] = qtd_entregue - qtd_desenho
                df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida'] = qtd_recebida - qtd_desenho
            else:
                df.loc[idx, 'qtd_recebida'] = qtd_recebida
                df.loc[idx, 'qtd_entregue'] = qtd_entregue - qtd_recebida
                df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida'] = 0
        else:
            df.iloc[[idx]]['qtd_recebida'] = 0
    return df


def get_quantities(df, df_warehouse):   
    df['qtd_recebida'] = 0
    for idx, row in df.iterrows():
        qtd_recebida = df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida']
        if not qtd_recebida.empty:
            qtd_lx = row['qtd_lx']
            qtd_recebida = qtd_recebida.iloc[0]
            if qtd_recebida >= qtd_lx:
                df.loc[idx, 'qtd_recebida'] = qtd_lx
                df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida'] = qtd_recebida - qtd_lx
            else:
                df.loc[idx, 'qtd_recebida'] = qtd_recebida
                df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_recebida'] = 0
        else:
            df.iloc[[idx]]['qtd_recebida'] = 0
    return df


def apply_status_fam(row):
    if (row['qtd_programacao'] + row['qtd_fabricacao'] + row['qtd_embarque'] + row['qtd_entregue'] + row['qtd_recebida']) < 1:
        status = '6.Inconsistente'
    elif row['order'] <= row['qtd_programacao']:
        status = '1.Programação'
    elif row['order'] <= row['qtd_programacao'] + row['qtd_fabricacao']:
        status = '2.Fabricação'
    elif row['order'] <= row['qtd_programacao'] + row['qtd_fabricacao'] + row['qtd_embarque']:
        status = '3.Embarque'
    elif row['order'] <= row['qtd_programacao'] + row['qtd_fabricacao'] + row['qtd_embarque'] + row['qtd_entregue']:
        status = '4.Enviado FAM'
    elif row['order'] <= row['qtd_programacao'] + row['qtd_fabricacao'] + row['qtd_embarque'] + row['qtd_entregue'] + row['qtd_recebida']:
        status = '5.Recebido VALE'
    else:
        status = '6.Inconsistente'
    return status


def apply_status_codeme(row):
    row = row.fillna({'qtd_recebida': 0})
    qtd_recebida = row['qtd_recebida']
    qtd_desenho = row['qtd_desenho']
    qtd_lx = row['qtd_lx']
    order = row['order']

    if not (qtd_desenho >= 0 or qtd_lx >= 0):
        status = '3.Não consta em LX'
    elif order <= qtd_recebida:
        status = '1.Recebido'
    elif order > qtd_recebida:
        status = '2.Não entregue'
    else:
        status = 'Erro de regra'
    return status


def apply_status_sinosteel(row):
    row = row.fillna({'qtd_recebida': 0})
    qtd_recebida = row['qtd_recebida']
    qtd_materials = row['qtd_desenho']
    qtd_lx = row['qtd_lx']
    order = row['order']

    if (row['color_b'] > row['color_g']) and (row['color_b'] > row['color_r']):
        status = '5.Não será detalhado'
    elif (row['color_r'] > row['color_g']) and (row['color_r'] > row['color_b']):
        status = '4.Aguardando detalhamento'
    elif not (qtd_materials >= 0 or qtd_lx >= 0):
        status = '3.Não consta em LX'
    elif order <= qtd_recebida:
        status = '1.Recebido'
    elif order > qtd_recebida:
        status = '2.Não entregue'
    else:
        status = 'Erro de regra'
    return status



def break_down_ifc_codeme(df):
    ativos = df['pwp'].drop_duplicates(keep='first')
    for ativo in ativos:
        sample_size = len(df.loc[df['pwp'] == ativo])

        if 11000 >= sample_size:
            quantil = pd.DataFrame(df.loc[df['pwp'] == ativo, 'elevation'].quantile([0, 1])).reset_index()

        if 20000 >= sample_size > 11000:
            quantil = pd.DataFrame(df.loc[df['pwp'] == ativo, 'elevation'].quantile([0, 0.5, 1])).reset_index()

        if 30000 >=  sample_size > 20000:
            quantil = pd.DataFrame(df.loc[df['pwp'] == ativo, 'elevation'].quantile([0, 0.25, 0.5, 0.75, 1])).reset_index()

        if sample_size > 30000:
            quantil = pd.DataFrame(df.loc[df['pwp'] == ativo, 'elevation'].quantile([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1])).reset_index()
        
        quantil['min_elevation'] = quantil['elevation'].shift(1)
        quantil = quantil.dropna().reset_index(drop=True)
        quantil.loc[0, 'min_elevation'] = -1

        for idx, row in quantil.iterrows():
            df.loc[(df['pwp'] == ativo) & (row['elevation'] >= df['elevation'].astype(float)) & (df['elevation'].astype(float) > row['min_elevation']), 'lvl'] = 'lvl ' + str(idx + 1).zfill(2)
    return df




def get_quantities_montagem_eletromecanica(df, df_warehouse, by):   
    df['qtd_solicitada'] = 0
    df['qtd_entregue'] = 0
    for idx, row in df.iterrows():
        qtd_distribuida = df_warehouse.loc[(df_warehouse[by[0]] == row[by[0]]) & (df_warehouse[by[1]] == row[by[1]]), ['qtd_solicitada', 'qtd_entregue']]
        qtd_faltante = row['qtd_desenho']
        if qtd_faltante > 0:
            if not qtd_distribuida.empty:
                qtd_solicitada = qtd_distribuida['qtd_solicitada'].iloc[0]
                qtd_entregue = qtd_distribuida['qtd_entregue'].iloc[0]
                if qtd_solicitada >= qtd_faltante:
                    df.loc[idx, 'qtd_solicitada'] = qtd_faltante
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_solicitada'] = qtd_solicitada - qtd_faltante
                else:
                    df.loc[idx, 'qtd_solicitada'] = qtd_solicitada
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_solicitada'] = 0

                if qtd_entregue >= qtd_faltante:
                    df.loc[idx, 'qtd_entregue'] = qtd_faltante
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_entregue'] = qtd_entregue - qtd_faltante
                else:
                    df.loc[idx, 'qtd_entregue'] = qtd_entregue
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_entregue'] = 0
    return df


def _predict_stock(df, df_warehouse):
    df = df.sort_values(by='data_inicio', ascending=True)
    df['qtd_solicitada_alocada'] = 0
    df['qtd_entregue_alocada'] = 0
    for idx, row in df.iterrows():
        qtd_disponivel = df_warehouse.loc[df_warehouse['tag'] == row['tag'], ['qtd_solicitada', 'qtd_entregue']]
        qtd_solicitada_faltante = row['qtd_desenho'] - row['qtd_solicitada']
        qtd_entregue_faltante = row['qtd_desenho'] - row['qtd_entregue']

        if qtd_solicitada_faltante > 0:
            if not qtd_disponivel['qtd_solicitada'].empty:
                qtd_solicitada_disponivel = qtd_disponivel['qtd_solicitada'].iloc[0]
                if qtd_solicitada_disponivel >= qtd_solicitada_faltante:
                    df.loc[idx, 'qtd_solicitada_alocada'] = qtd_solicitada_faltante
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_solicitada'] = qtd_solicitada_disponivel - qtd_solicitada_faltante
                else:
                    df.loc[idx, 'qtd_solicitada_alocada'] = qtd_solicitada_disponivel
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_solicitada'] = 0

        if qtd_entregue_faltante > 0:
            if not qtd_disponivel['qtd_entregue'].empty:
                qtd_entregue_disponivel = qtd_disponivel['qtd_entregue'].iloc[0]
                if qtd_entregue_disponivel >= qtd_entregue_faltante:
                    df.loc[idx, 'qtd_entregue_alocada'] = qtd_entregue_faltante
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_entregue'] = qtd_entregue_disponivel - qtd_entregue_faltante
                else:
                    df.loc[idx, 'qtd_entregue_alocada'] = qtd_entregue_disponivel
                    df_warehouse.loc[df_warehouse['tag'] == row['tag'], 'qtd_entregue'] = 0
    return df

