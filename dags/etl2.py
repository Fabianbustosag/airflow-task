import pandas as pd
import xml.etree.ElementTree as ET

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

def process_chunk(df_chunk, filas_con, filas_sin):
    for idx, row in df_chunk.iterrows():
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
    return filas_con, filas_sin

def func1(df, filas_con, filas_sin):
    for i in range(0, len(df), 15):
        df_chunk = df.iloc[i:i+5]
        filas_con, filas_sin = process_chunk(df_chunk, filas_con, filas_sin)
    return filas_con, filas_sin

def func2(df, filas_con, filas_sin):
    for i in range(5, len(df), 15):
        df_chunk = df.iloc[i:i+5]
        filas_con, filas_sin = process_chunk(df_chunk, filas_con, filas_sin)
    return filas_con, filas_sin

def func3(df, filas_con, filas_sin):
    for i in range(10, len(df), 15):
        df_chunk = df.iloc[i:i+5]
        filas_con, filas_sin = process_chunk(df_chunk, filas_con, filas_sin)
    return filas_con, filas_sin

def df_transform(df):
    filas_sin = []
    filas_con = []

    # Ejecutar las funciones paralelamente
    filas_con, filas_sin = func1(df, filas_con, filas_sin)
    filas_con, filas_sin = func2(df, filas_con, filas_sin)
    filas_con, filas_sin = func3(df, filas_con, filas_sin)

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
    # return print(df_new_sorted.info())
    return df_new_sorted


def df_transform2(df):
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

    # Recorrer el dataframe de 5 en 5 filas, saltando 10 filas entre cada grupo
    i = 0
    while i < len(df):
        # Leer 5 filas
        for j in range(5):
            if i + j >= len(df):
                break

            row = df.iloc[i + j]
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

        # Saltar 10 filas
        i += 15

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
    
