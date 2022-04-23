pip install psycopg2
pip install pandas

import requests
import json
import pandas as pd
import psycopg2

# Reading csv file
dados = pd.read_csv("directory_file", sep = ",", header = 0, index_col = False)

# Create a DataFrame
df = pd.DataFrame(dados)

# All columns to String
for col in df.columns:
  df[col] = df[col].apply(str)

# Creating a DB conection
def conecta_db():
  con = psycopg2.connect(host='hostname', 
                         database='database name',
                         user='username',                   
                         password='keypass')
  return con

# Function to create table on DB
def criar_db(sql):
  con = conecta_db()
  cur = con.cursor()
  cur.execute(sql)
  con.commit()
  con.close()

# Drop table if exists
sql = 'DROP TABLE IF EXISTS a33b7b.VALOR_INDICADOR'
criar_db(sql)
# Criando a tabela do hospital
sql = '''CREATE TABLE XXXXX 
      ( XXXXXX          character varying(10), 
        XXXXXX          character varying(100), 
        XXXXXX          character varying(500), 
        XXXXXX          character varying(50), 
        XXXXXX          character varying(100)
      )'''
criar_db(sql)

# Function to insert on DB
def inserir_db(sql):
    con = conecta_db()
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()

# Inserting each record of the DataFrame
for i in df.index:
    sql = """
    INSERT into XXXXXX (XXXXXX, XXXXXX, XXXXXX, XXXXXX) 
    values('%s','%s','%s','%s');
    """ % (df['XXXXXX'][i], df['XXXXXX'][i], df['XXXXXX'][i], df['XXXXXX'][i])
    inserir_db(sql)