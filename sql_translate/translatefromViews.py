import logging
from impala.dbapi import connect
from tabulate import tabulate
from sql_metadata import Parser
import pandas as pd
import os
from sql_translate import translation
import json
import logging
import re

conf = open(r'./sql_translate/lineage/lineage_config.json')
data = json.load(conf)
conn = connect(host=data['host'],port=data['port'],database=data['database'])
cursor = conn.cursor()

path = open(r'./config/database.json')
database_name = json.load(path)

inputpath = 'impala queries'
if not os.path.exists(inputpath):
        os.makedirs(inputpath) 
        
outputpath = 'presto queries'
if not os.path.exists(outputpath):
        os.makedirs(outputpath) 
 
def get_create_statement(entity_name,mapping_dict):
    try:
        cursor.execute(f"SHOW CREATE TABLE {entity_name}")
        result = cursor.fetchall()
        result_string = result[0][0]
        if result_string.find('VIEW')!=-1:                                  # only views have a select statement
            impala_select_statement = result_string.partition("AS")[2]
            with open(f'{inputpath}/{entity_name}.txt', 'w') as f:
                f.write(impala_select_statement)

            try:    
                ImpalaToPresto = translation.ImpalaToPresto()
                translated_sql_statement = ImpalaToPresto.translate_statement(impala_select_statement, has_insert_statement=False) + ' '
                for key in mapping_dict:
                        translated_sql_statement = re.sub(rf'\b{key}\b',mapping_dict[key],translated_sql_statement)
                
                with open(f'{outputpath}/{entity_name}.txt', 'w') as f:
                    f.write(translated_sql_statement)

                return impala_select_statement, translated_sql_statement
            
            except Exception as e:
                 logging.exception(f'Failed tranlation of {entity_name}')
                 return '',''
        
        else:
             return '',''

    except Exception as e:
         logging.exception(f'{entity_name} not found in impala')
         return '',''

def create_select_statements(df2,p_value=None,s_value=None):
     logging.info("x--------------------Starting View Translator Module--------------------x")
     view_names = df2["impala object name"].values.tolist()
     impala_names = df2["impala object name"].values.tolist()
     if p_value is None and s_value is None: 
        presto_names = df2["presto object name"].values.tolist()

     mapping_dict = {}
     for i in range(len(impala_names)):
        key = impala_names[i].strip()
        if p_value != None:
            mapping_dict[key] = p_value + impala_names[i].strip()
        elif s_value != None:
            mapping_dict[key] = impala_names[i].strip() + s_value
        elif p_value is None and s_value is None:
            mapping_dict[key] = presto_names[i]
            
     mapping_dict[database_name['impala_database']] = database_name['presto_database']

     data ={
            'query' :[],
            'translated_query':[]
            }

     for i in view_names:
         impala_select_statement, presto_select_statement = get_create_statement(i,mapping_dict)
         if impala_select_statement :
            data['query'].append(impala_select_statement)
            data['translated_query'].append(presto_select_statement)
    
     df3 = pd.DataFrame.from_dict(data)
     logging.info("x--------------------View Translator Module Ended--------------------x")
     return df3