import psycopg2
import pandas as pd
from query_sql import query_POSTGRESQL, DataFrame2DataBaseStatic
import os, sys
import xml.etree.ElementTree as ET

# Devuelve el dataframe
def extract_data():
    # Consulta SQL
    query_sql = '''SELECT * FROM dtes'''
    # Extraer datos a un DataFrame
    df = query_POSTGRESQL(query_sql)
    
    # Información del DataFramel
    # print(f'extract data {df.info()}')
    # print('--------------------------------------------')

    return df

def get_element_value(element, tag):
    ns = {'ns': 'http://www.sii.cl/SiiDte'}
    return element.find(f'ns:{tag}', ns).text if element.find(f'ns:{tag}', ns) is not None else None

# Funcion para combinar columnas
def combinar_columnas(df, col1, col2, nueva_columna):
    df[nueva_columna] = df[col1].astype(str) + " " + df[col2].astype(str)
    df = df.drop(columns=[col1, col2])
    return df

# Funcion para cambiar de posicion columnas
def cambiar_lugar_columnas(df, col1, col2):
    if col1 in df.columns and col2 in df.columns:
        cols = list(df.columns)
        col1_index, col2_index = cols.index(col1), cols.index(col2)
        
        # Intercambiar los nombres de las columnas en la lista
        cols[col1_index], cols[col2_index] = cols[col2_index], cols[col1_index]
        
        # Reordenar el DataFrame según la nueva lista de columnas
        df = df[cols]
    else:
        print(f"Una o ambas columnas '{col1}' y '{col2}' no existen en el DataFrame.")
    return df

# Param
# start_offset = en que fila va a empezar
# Luego va a leer en bloques de 5 y se saltara 10 porque son 3 funciones
# entrega el dataframe saltandose indices
def funcion_paralela(df,start_offset):
    ns = {'ns': 'http://www.sii.cl/SiiDte'}
    filas_sin = []
    filas_con = []

    block_size = 5
    skip_size = 10
    # start_offset = 10
    total_rows = len(df)

    for start_idx in range(start_offset, total_rows, block_size + skip_size):
        end_idx = min(start_idx + block_size, total_rows)
        df_block = df.iloc[start_idx:end_idx]

        for idx, row in df_block.iterrows():
            xml = row['convert_from']
            ID = row['id']

            root = ET.fromstring(xml)
            Folio = root.find('.//{http://www.sii.cl/SiiDte}Folio').text
            RutEmisor = root.find('.//{http://www.sii.cl/SiiDte}RutEmisor').text
            detalles = root.findall('.//ns:Detalle', ns)

            for detalle in detalles:
                NroLinDet = get_element_value(detalle, 'NroLinDet')
                NmbItem = get_element_value(detalle, 'NmbItem')
                MontoItem = get_element_value(detalle, 'MontoItem')
                DscItem = get_element_value(detalle, 'DscItem')
                QtyItem = get_element_value(detalle, 'QtyItem')
                n_item = QtyItem if QtyItem is not None else '1'

                nuevaFila = {
                    "id": ID,
                    "rut_emisor": RutEmisor,
                    "folio": Folio,
                    "n_item": n_item,
                    "nmb_item": NmbItem,
                    "dsc_item": DscItem if DscItem is not None else None,
                    "monto_item": MontoItem,
                }
                if DscItem is not None:
                    filas_con.append(nuevaFila)
                else:
                    filas_sin.append(nuevaFila)

    df_con = pd.DataFrame(filas_con)
    df_sin = pd.DataFrame(filas_sin)
    df_sin = df_sin.drop(columns=['dsc_item'])
    df_sin = df_sin.rename(columns={'nmb_item': 'glosa'})
    df_con = combinar_columnas(df_con, 'nmb_item', 'dsc_item', 'glosa')
    df_con = cambiar_lugar_columnas(df_con, 'monto_item', 'glosa')
    df_new = pd.concat([df_con, df_sin], axis=0, ignore_index=False)

    return df_new

def task_etl(start_offset):
    # extraccion
    df = extract_data()
    # transformacion
    df_new = funcion_paralela(df,start_offset)
    # carga
    table_name = 'mendoza_bustos_fonseca_ramos'
    DataFrame2DataBaseStatic(dataframe=df_new, table_name=table_name, schema='airflow')
