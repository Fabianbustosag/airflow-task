import psycopg2
import pandas as pd
from query_sql import query_POSTGRESQL 
import os, sys
import xml.etree.ElementTree as ET
from query_sql import query_POSTGRESQL, DataFrame2DataBase

# Especifica la ruta al archivo Excel
# archivo_excel = 'extract_glosa.xlsx'

path_files = os.getcwd()
# cambiar Tarea1 por el nombre de su carpeta
path_sql = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir), "Tarea1") 
sys.path.insert(0, path_sql)

# -------------------------  EXTRACCION DE DATOS
query_sql = '''SELECT * FROM dtes'''
# tener en la misma carpeta in archivo llamado database.ini
df = query_POSTGRESQL(query_sql)

# ------------------------- TRANSFORMACION DE DATOS

ns = {'ns': 'http://www.sii.cl/SiiDte'}

# Funcion para obtener un elemento
def get_element_value(element, tag):
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
        print(f"Columnas '{col1}' y '{col2}' han sido intercambiadas.")
    else:
        print(f"Una o ambas columnas '{col1}' y '{col2}' no existen en el DataFrame.")
    return df

filas_sin = []
filas_con = []

# Recorrer el dataframe
for idx, row in df.iterrows():
    xml = row['convert_from']
    ID = row['id']  # Obtener el ID directamente del DataFrame original

    root = ET.fromstring(xml)
    namespace = {'default': 'http://www.sii.cl/SiiDte'}

    # Obtener los valores "globales" del xml
    Folio = root.find('.//{http://www.sii.cl/SiiDte}Folio').text
    RutEmisor = root.find('.//{http://www.sii.cl/SiiDte}RutEmisor').text

    # Obtener todos los detalles del xml <detalle>
    detalles = root.findall('.//ns:Detalle', ns)
    
    # Recorrer cada detalle de los detalles
    for detalle in detalles:
        NroLinDet = get_element_value(detalle, 'NroLinDet')
        NmbItem = get_element_value(detalle, 'NmbItem')
        MontoItem = get_element_value(detalle, 'MontoItem')
        DscItem = get_element_value(detalle, 'DscItem')
        QtyItem = get_element_value(detalle, 'QtyItem')
        
        # Si QtyItem es None, asignar el valor 1
        n_item = QtyItem if QtyItem is not None else '1'

        # Crear una fila para agregar al dataframe
        # Si esta el elemento <DscItem> se agrega al df que contiene las intancias con DscItem
        if DscItem is not None:
            nuevaFila = {
                "id": ID,
                "rut_emisor": RutEmisor,
                "folio": Folio,
                "n_item": n_item,
                "nmb_item": NmbItem,
                "dsc_item": DscItem,
                "monto_item": MontoItem,
            }
            filas_con.append(nuevaFila)
        # Si no esta el elemento <DscItem> se agrega la fila al df que no tiene 
        else:
            nuevaFila = {
                "id": ID,
                "rut_emisor": RutEmisor,
                "folio": Folio,
                "n_item": n_item,
                "nmb_item": NmbItem,
                "dsc_item": None,
                "monto_item": MontoItem,
            }
            filas_sin.append(nuevaFila)

# agregar las nuevas filas a los distintos df
df_con = pd.DataFrame(filas_con) # df con DscItem
df_sin = pd.DataFrame(filas_sin) # df sin DscItem


print('--------------------------------')
print(f'incio ----- {df_sin.info()} ------- fin')
print('--------------------------------')
# Dejar df_sin <DscItem> con el formato requerido
# Elimino la columna dsc_item porque no tiene nada en ese campo
df_sin = df_sin.drop(columns=['dsc_item'])
# Renombro las columna nmb_item que es la unica glosa que hay
df_sin = df_sin.rename(columns={'nmb_item': 'glosa'})

# Dejar df_con <DscItem> con el formato requerido
df_con = combinar_columnas(df_con, 'nmb_item', 'dsc_item', 'glosa')
df_con = cambiar_lugar_columnas(df_con, 'monto_item', 'glosa')

# Juntar los dataframe
df_new = pd.concat([df_con, df_sin], axis=0, ignore_index=False)

# Establecer la columna 'id' como índice
df_new.set_index('id', inplace=True)

# Agregar indice numérico si es necesario
df_new = df_new.reset_index()

# Ordenar el DataFrame final de mayor a menor según el valor de 'id'
df_new_sorted = df_new.sort_values(by='id', ascending=False)

# Imprimir el DataFrame ordenado
# print(df_new_sorted.head())

# Imprimir la informacion del DataFrame
# print(df_new_sorted.info())

# Si el script salio bien debe dar este resultado
# Columnas 'monto_item' y 'glosa' han sido intercambiadas.
#         id  rut_emisor     folio  n_item                                              glosa monto_item
# 0    99760  77566800-8       440  140.00  Arriendo local 2 Puerto montt    valor proporc...    5156309
# 1    98762  76124336-5      1926    1.00         ARRIENDO  ENERO 2024 VILLAGRAN 571 MULCHEN    1104923
# 535  98474  76113783-2    284267       1                                    CL WEX pack 120      68899
# 537  98459  96806980-2  48661220       1                      SERVICIO DE ACCESO A INTERNET     170088
# 538  98459  96806980-2  48661220       1                          Servicio Telefonico Movil     772545
# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 1102 entries, 0 to 1101
# Data columns (total 6 columns):
#  #   Column      Non-Null Count  Dtype
# ---  ------      --------------  -----
#  0   id          1102 non-null   int64
#  1   rut_emisor  1102 non-null   object
#  2   folio       1102 non-null   object
#  3   n_item      1102 non-null   object
#  4   glosa       1102 non-null   object
#  5   monto_item  1102 non-null   object
# dtypes: int64(1), object(5)
# memory usage: 51.8+ KB
# None
# -------------------------  CARGA DE DATOS

# esto es para subirlo a la base de datos al schema python
# table_name = 'mendoza_bustos_fonseca_ramos'
# DataFrame2DataBaseStatic(path_sql, 'USM', df_new_sorted, table_name, 'python')