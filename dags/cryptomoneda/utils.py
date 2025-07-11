from datetime import datetime, timedelta
import time
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras as extras 
import os
from dotenv import load_dotenv


def etl():
    # Load dataframe    
    df = load_api_dataframe()
    df.to_csv('./crypto.csv', index = False)
    
    return 'cryto.csv send succeded'

def load_datawarehouse():
    fecha = pd.to_datetime('today').strftime("%Y-%m-%d")
    
    conn = conectar_bd()
    # Load CSV en dataframe
    df = pd.read_csv('./crypto.csv')
    print(df.head(5))
    
    # CARGA INCREMENTAL

    # Eliminando registros de esa "fecha"
    query=f""" DELETE FROM cryptomoneda WHERE fecha = '{fecha}'; """
    runExec(conn, query)

    # Añadiendo el dataframe a la tabla de la BD
    runExecMany(conn, df, 'cryptomoneda')    
    
    return 'Carga incremental exitoso'

def conectar_bd():
    load_dotenv()

    hostname= os.environ.get('_HOSTNAME')
    database= os.environ.get('_DATABASE')
    username= os.environ.get('_USER')
    pwd= os.environ.get('_PWD')
    port_id= os.environ.get('_PORT_ID')
    
    print(f'Configuracion BD >> hostname: {hostname}, database:{database}, username: {username}, port: {port_id}')

    try:
        connexion = psycopg2.connect(host=hostname, dbname=database, user=username, password=pwd, port=port_id)
            
    except Exception as e:
        print(f"Error '{e}' ha ocurrido")
          
    print(f'Conexion: {connexion}')
    return connexion

def load_api_dataframe():
    # URL de la API
    url_crypto = 'https://api.coincap.io/v2/assets'
    response = requests.get(url_crypto).json()
    
    # Obteniendo la fecha
    # fecha = datetime.fromtimestamp(response['timestamp'] / 1000).strftime("%Y-%m-%d")
    fecha = pd.to_datetime('today').strftime("%Y-%m-%d")
    
    # Obteniendo en diccionario la lista de ranking de crytomonedas
    datos = []
    for item in response['data']:
        name = item['name']
        rank = int(item['rank'])
        priceUsd = round(float(item['priceUsd']), 2)
        datos.append((fecha, name, priceUsd, rank))    
    
    # Creando el dataframe
    col = ['fecha', 'nombre','precio','ranking']
    df = pd.DataFrame(datos,columns=col)
    df = df.sort_values(by = 'ranking',ascending = True)
    return df

def runQuery(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        columnas = [description[0] for description in cursor.description]
        result = cursor.fetchall()
        return pd.DataFrame(result, columns=columnas)
    except Exception as e:
        print(f"Error '{e}' ha ocurrido")
        
def runExec(connection, query):
    cursor = connection.cursor()
    try:
        result = cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(f"Error '{e}' ha ocurrido")

def runExecMany(connection, df, table): 
    tuples = [tuple(x) for x in df.to_numpy()] 
    cols = ','.join(list(df.columns)) 
    # SQL query to execute 
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
    cursor = connection.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        connection.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        connection.rollback() 
        cursor.close() 
        return 1
    print("the dataframe is inserted") 
    cursor.close() 
    
