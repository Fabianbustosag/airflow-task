import psycopg2
from configparser import ConfigParser
import pandas as pd
import os
import re
from sqlalchemy import create_engine
import urllib.parse
import pymysql.cursors
import pymssql

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # print(f'parser.read(filename): {parser.read(filename)}')

    # get section, default to postgresql
    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    
    return db
        
#%%QUERY GENERAL

database_config = {
    'host': '10.33.195.214',
    'database': 'pmd',
    'user': 'admin',
    'password': 'admin',
    'port': '5432'
}

def DataFrame2DataBaseStatic(dataframe, table_name, schema):

    database_config = {
    'host': '10.33.195.214',
    'database': 'pmd',
    'user': 'admin',
    'password': 'admin',
    'port': '5432'
    }
    # CÓDIFICA CARACTERES ESPECIALES DE DATABASE, USUARIO Y PASSWORD
    database = urllib.parse.quote_plus(database_config['database'])
    user = urllib.parse.quote_plus(database_config['user'])
    password = urllib.parse.quote_plus(database_config['password'])
    host = database_config['host']
    port = database_config['port']
    
    # CREA LA CADENA DE CONEXIÓN
    engine = create_engine(f'postgresql://{user}:{password}@{host}/{database}')
    
    # ALMACENA DF EN DB
    dataframe.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)

#POSTGRESQL
def query_POSTGRESQL(queries, extract_columns_types = False):  
    #QUERY A SQL
    # params = config(os.path.join(path_sql, 'database.ini'), database)
    # conn = psycopg2.connect(**params)
    # cur = conn.cursor()

    conn = psycopg2.connect(**database_config)
    cur = conn.cursor()
    
    #TRANSFORMA QUERY A LIST SI ES NECESARIO
    if type(queries) != list:
        queries = [queries]
    
    #EJECUTA QUERIES
    list_out = []
    for query in queries:
        data_out = pd.DataFrame()
    
        #SELECT
        if re.split(' |\n', query)[0].upper() == 'SELECT' or re.split(' |\n', query)[0].upper() == 'WITH':
            cur.execute(query)
            out = cur.fetchall()
            
            #EXTRAE COLUMNAS
            col_names = []
            col_types = []
            for elt in cur.description:
                col_names.append(elt[0])
                col_types.append(elt[1])
            
            #DATOS A DATAFRAME
            data_out = pd.DataFrame(out, columns=col_names)
            if extract_columns_types == True:
                data_types = pd.DataFrame({'col_name':col_names, 'col_oid_types':col_types})
                
                #TRANSFORMA OID A TEXTO
                col_extract = str((data_types['col_oid_types'].drop_duplicates(keep='first').astype(str) + '::regtype::text AS "' + data_types['col_oid_types'].drop_duplicates(keep='first').astype(str) + '"').tolist()).replace('[','').replace(']','').replace("'",'')
                query_sql = '''SELECT %s'''%(col_extract)
                
                #EXTRAE TEXTO DE OID
                cur.execute(query_sql)
                out = cur.fetchall()
                
                #CRUZA CON DATAFRAME DE TIPOS
                out = pd.DataFrame(out, columns = data_types['col_oid_types'].drop_duplicates(keep='first').tolist()).T.rename(columns={0:'col_types'}).reset_index()
                data_types = pd.merge(data_types, out, how='left', left_on='col_oid_types', right_on='index').drop(['col_oid_types','index'], axis=1)

            list_out.append(data_out)

        #UPDATE Y DELETE
        elif re.split(' |\n', query)[0].upper()  == 'UPDATE' or re.split(' |\n', query)[0].upper()  == 'DELETE' or re.split(' |\n', query)[0].upper()  == 'CREATE' or re.split(' |\n', query)[0].upper()  == 'TRUNCATE' or re.split(' |\n', query)[0].upper()  == 'INSERT' or re.split(' |\n', query)[0].upper()  == 'DROP':
            cur.execute(query)
            conn.commit()
            
        #FUNCIONES
        elif re.split(' |\n', query)[0].upper() == 'FUNCION':
            query = query.lstrip('FUNCION ')
            cur.execute(query)
            conn.commit()

        else:
            raise Exception('Query no definida')
    
    if 'data_types' in locals():
        conn.close()
        
        if len(list_out) == 1:
            return list_out[0], data_types
        elif len(list_out) > 1:
            return list_out, data_types
    
    else:
        conn.close()
        
        if len(list_out) == 1:
            return list_out[0]
        elif len(list_out) > 1:
            return list_out
        
#MYSQL
def query_mysql(path_sql, database, query):

    params = config(os.path.join(path_sql, 'database.ini'), database)
    connection = pymysql.connect(**params, cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

            #EXTRAE COLUMNAS
            col_names = []
            for elt in cursor.description:
                col_names.append(elt[0])

            out  = cursor.fetchall()
            out = pd.DataFrame(out, columns = col_names)

            return out

#SQL SERVER
def query_sqlserver(path_sql, database, queries, extract_columns_types = False):

    params = config(os.path.join(path_sql, 'database.ini'), database)
    conn = pymssql.connect(**params)
    cur = conn.cursor()

    #TRANSFORMA QUERY A LIST SI ES NECESARIO
    if type(queries) != list:
        queries = [queries]

    #EJECUTA QUERIES
    list_out = []
    for query in queries:
        data_out = pd.DataFrame()

        #SELECT
        if re.split(' |\n', query)[0].upper() == 'SELECT' or re.split(' |\n', query)[0].upper() == 'WITH':
            cur.execute(query)
            out = cur.fetchall()

            #EXTRAE COLUMNAS
            col_names = []
            col_types = []
            for elt in cur.description:
                col_names.append(elt[0])
                col_types.append(elt[1])

            #DATOS A DATAFRAME
            data_out = pd.DataFrame(out, columns=col_names)
            if extract_columns_types == True:
                data_types = pd.DataFrame({'col_name':col_names, 'col_oid_types':col_types})

                #TRANSFORMA OID A TEXTO
                col_extract = str((data_types['col_oid_types'].drop_duplicates(keep='first').astype(str) + '::regtype::text AS "' + data_types['col_oid_types'].drop_duplicates(keep='first').astype(str) + '"').tolist()).replace('[','').replace(']','').replace("'",'')
                query_sql = '''SELECT %s'''%(col_extract)

                #EXTRAE TEXTO DE OID
                cur.execute(query_sql)
                out = cur.fetchall()

                #CRUZA CON DATAFRAME DE TIPOS
                out = pd.DataFrame(out, columns = data_types['col_oid_types'].drop_duplicates(keep='first').tolist()).T.rename(columns={0:'col_types'}).reset_index()
                data_types = pd.merge(data_types, out, how='left', left_on='col_oid_types', right_on='index').drop(['col_oid_types','index'], axis=1)

            list_out.append(data_out)

        #UPDATE Y DELETE
        elif re.split(' |\n', query)[0].upper()  == 'UPDATE' or re.split(' |\n', query)[0].upper()  == 'DELETE' or re.split(' |\n', query)[0].upper()  == 'CREATE' or re.split(' |\n', query)[0].upper()  == 'TRUNCATE' or re.split(' |\n', query)[0].upper()  == 'INSERT' or re.split(' |\n', query)[0].upper()  == 'DROP':
            cur.execute(query)
            conn.commit()

        #FUNCIONES
        elif re.split(' |\n', query)[0].upper() == 'FUNCION':
            query = query.lstrip('FUNCION ')
            cur.execute(query)
            conn.commit()

        else:
            raise Exception('Query no definida')

    if 'data_types' in locals():
        conn.close()

        if len(list_out) == 1:
            return list_out[0], data_types
        elif len(list_out) > 1:
            return list_out, data_types

    else:
        conn.close()

        if len(list_out) == 1:
            return list_out[0]
        elif len(list_out) > 1:
            return list_out


        
#%%INSERT DATAFRAME TO DATABASE

def DataFrame2DataBase(path_sql, database, dataframe, table_name, schema):
    #EXTRAE CREDENCIALES
    params = config(os.path.join(path_sql, 'database.ini'), database)
    
    #CÓDIFICA CARACTERES ESPECIALES DE DATABASE, USUARIO Y PASSWORD
    params['database'] = urllib.parse.quote_plus(params['database'])
    params['user'] = urllib.parse.quote_plus(params['user'])
    params['password'] = urllib.parse.quote_plus(params['password'])
    
    #ALMACENA DF EN DB
    engine = create_engine('postgresql://'+params['user']+':'+params['password']+'@'+params['host']+'/'+params['database'])
    dataframe.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)
    
