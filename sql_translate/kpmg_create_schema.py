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

def function_alias(query):
    from_part = query.partition('FROM')[2]            # part of query after FROM 
    s = query.partition('FROM')[0]            
    s = s.strip() + ','                       

    i=0
    j=0
    k=1
    prev_char=''
    flag = True

    for c in s:
        if i==0 and prev_char==' ' and c=='(':          
            flag=False                                      #  if ' ' detected before ( then its not a function

        if c=='(':
            i+=1

        if c==')':
            i-=1

            if i==0 and flag==True:
    #             print('function end detected')
                sub_len=0
                for x in s[j+1:]:                            
                    if x==',':                               
                        break
                    sub_len+=1
                substr = s[j+1:j+1+sub_len]

                if substr.strip() == '' or sub_len==0:               # give alias if not present
                    alias = 'col' + f'{k}'
                    s = s[:j+1] + ' ' + alias + s[j+1:]
                    k+=1
                    j=j+len(alias)+1

            elif i==0 and flag==False:
                flag=True

        j=j+1
        prev_char = c

    s = s[:-1]                                   # to remove the comma that was added
    final_query = s + ' ' +'FROM' + from_part
    return final_query,k


def case_alias(query,k):
    from_part = query.partition('FROM')[2]           
    s = query.partition('FROM')[0]             
    s = s.strip() + ','                      

    i=0
    j=0
    flag = False

    for c in s:
        if c=='C' and s[j:j+4]=='CASE':                           # CASE detected
            flag = True

        if c=='E' and s[j:j+3]=='END' and flag==True:             # END detected
            sub_len=0
            for x in s[j+3:]:
                if x == ',':
                    break
                sub_len+=1

            alias_present = s[j+3:j+3+sub_len]

            if alias_present.strip()=='' or sub_len==0:
                alias = 'col' + f'{k}'
                s = s[:j+3] + ' ' + alias + s[j+3:]
                k+=1
                j=j+len(alias)+1

        j=j+1

    s = s[:-1]                                           #to remove the comma that was added
    final_query = s + ' ' +'FROM' + from_part
    return final_query


def get_create_statement(entity_name,mapping_dict):

    try:
        cursor.execute(f"SHOW CREATE TABLE {entity_name}")
        result = cursor.fetchall()
        result_string = result[0][0]

        if result_string.find('TABLE')!=-1:
            x = result_string.replace("STRING","VARCHAR").replace(" INT"," INTEGER")\
            .replace("REAL","DOUBLE").replace("FLOAT","REAL").replace("<INT>","<INTEGER>")\
            .replace("<STRING>","<VARCHAR>").replace("<REAL>","<DOUBLE>").replace("<FLOAT>","<REAL>")
            x = x.replace(f'{entity_name}',mapping_dict[entity_name])
            x = x.replace(database_name['impala_database'],database_name['presto_database'])
            with open(f'{outputpath}/{entity_name}.txt', 'w') as f:
                f.write(x)

        else:
            try:
                impala_select_statement = result_string.partition("AS")[2]
                ImpalaToPresto = translation.ImpalaToPresto()
                translated_sql_statement = ImpalaToPresto.translate_statement(impala_select_statement, has_insert_statement=False)
                presto_create_statement = result_string.partition("AS")[0].replace('`','"') + result_string.partition("AS")[1] + ' ' + translated_sql_statement
                for key in mapping_dict:
                    presto_create_statement = re.sub(rf'\b{key}\b',mapping_dict[key],presto_create_statement)
                
                presto_create_statement,k = function_alias(presto_create_statement)
                presto_create_statement = case_alias(presto_create_statement,k)
                with open(f'{outputpath}/{entity_name}.txt', 'w') as f:
                    f.write(presto_create_statement)

            except Exception as e:
                logging.exception(f'Failed translation of {entity_name}')
        
    except:
         logging.exception(f"{entity_name} not found in impala")

outputpath = 'presto_create_statements'
if not os.path.exists(outputpath):
        os.makedirs(outputpath) 

def create_presto_statements(df2,p_value=None,s_value=None):
    logging.info("x--------------------Starting Schema Generator Module--------------------x")
    tables_and_views = df2["impala object name"].values.tolist()
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
    
    for i in tables_and_views:
        get_create_statement(i,mapping_dict)

    logging.info("x--------------------Schema Generator Module Ended--------------------x")