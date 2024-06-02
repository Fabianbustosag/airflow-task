from query_sql import query_POSTGRESQL, DataFrame2DataBaseStatic
import xml.etree.ElementTree as ET
import pandas as pd

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

# Transformacion de datos
def df_transform(df):
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
            # print(f"Columnas '{col1}' y '{col2}' han sido intercambiadas.")
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

    # Dejar df_sin <DscItem> con el formato requerido {df_sin.info()}
    # Elimino la columna dsc_item porque no tiene nada en ese campo
    print('--------------------------------')
    # print(f'incio ----- {df_sin.info()} ------- fin')
    print('--------------------------------')


    if 'dsc_item' in df_sin.columns:
        df_sin = df_sin.drop(columns=['dsc_item'])
    # Renombro las columna nmb_item que es la unica glosa que hay
    df_sin = df_sin.rename(columns={'nmb_item': 'glosa'})

    # Dejar df_con <DscItem> con el formato requerido
    if 'nmb_item' in df_con.columns: 
        df_con = combinar_columnas(df_con, 'nmb_item', 'dsc_item', 'glosa')
    df_con = cambiar_lugar_columnas(df_con, 'monto_item', 'glosa')

    # Juntar los dataframe
    df_new = pd.concat([df_con, df_sin], axis=0, ignore_index=False)

    # Establecer la columna 'id' como índice
    # df_new.set_index('id', inplace=True)

    # Agregar indice numérico si es necesario
    # df_new = df_new.reset_index()

    # Ordenar el DataFrame final de mayor a menor según el valor de 'id'
    # df_new_sorted = df_new.sort_values(by='id', ascending=False)
    # return print(df_new_sorted.info())
    return df_new.info()



# Funcion que hace las transformaciones de las filas del df que le toca
# Param inicio, en que fila del df va a empezar
def funcion_paralela(df, inicio, leer):
    # Inicializamos una lista para almacenar los DataFrames seleccionados
    lista_dfs = []
    
    # Recorremos el DataFrame con un paso de leer * 3 (desfasado para tres funciones)
    for i in range(inicio, len(df), leer * 3):
        # Seleccionamos las filas correspondientes
        df_leido = df.iloc[i:i+leer]
        
        # Aplicamos la transformación df_transform a las filas leídas
        df_transformado = df_transform(df_leido)
        
        # Añadimos las filas transformadas a la lista
        lista_dfs.append(df_transformado)
    
    # Concatenamos los DataFrames transformados en uno solo
    df_resultado = pd.concat(lista_dfs, ignore_index=True)

    return df_resultado


# Función envoltorio para el PythonOperator
def airflow_task(inicio, leer):
    # Supongamos que el DataFrame se obtiene de algún lugar, por ejemplo, una lectura desde un archivo CSV
    df = extract_data()
    
    # Funcion paralela transforma los datos y entrega
    result = funcion_paralela(df, inicio, leer)

    return result

# df_transform(extract_data())


# airflow_task(inicio=0, leer=5)
# airflow_task(inicio=5, leer=5)
# airflow_task(inicio=10, leer=5)
funcion_paralela(extract_data(), inicio=0, leer=5)






# df_transform(extract_data())