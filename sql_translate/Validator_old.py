# -*- coding: utf-8 -*-
"""
Created on Thu May 25 13:43:18 2023

@author: salonydubey
"""

import pandas as pd
from presto import *
from pyhive import presto 
from impala.dbapi import connect as impala_connect
import logging
from datetime import datetime
import time 

# logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

statistics = {
    'executionid': '',
    'filename': '',
    'rowcount_impala': '',
    'rowcount_presto': '',
    'validaterows': False,
    'execution_time_impala (sec)': '',
    'execution_time_presto (sec)': '',
    'variation_time (sec)': '',
    'schema_check': False,
    'numeric_columns_impala': [],
    'numeric_columns_presto': [],
    'mean_impala': '',
    'mean_presto': '',
    'sum_impala' :'',
    'sum_presto' :'',
    'min_impala' :'',
    'min_presto' :'',
    'max_impala' :'',
    'max_presto':'',
    'String_len_validation' : '',
    'validate_datetime_format': ''
}

final_df = []


def connectImpala(impalaconnection):
    try:
        # Connect to Impala
        impala_conn = impala_connect(host=impalaconnection["host"], port=impalaconnection["port"])
        impala_cursor = impala_conn.cursor()
        log.info("Impala connection created successfully")
        return impala_cursor
    except Exception as e:
        log.error("Error connecting to Impala: %s", str(e))


def connectPresto(prestoconnection):
    try:
        # Connect to Presto
        presto_conn = presto.connect(host=prestoconnection["host"], port=prestoconnection["port"],
                                     catalog=prestoconnection["catalog"], schema=prestoconnection["schema"])
        presto_cursor = presto_conn.cursor()
        log.info("Presto connection created successfully")
        return presto_cursor
    except Exception as e:
        log.error("Error connecting to Presto: %s", str(e))


def save_csv(path, output_file, final_df):
    try:
        df = pd.DataFrame(final_df)
        df.to_csv(path + '\\' + output_file, index=False)
        log.info("Stats file saved successfully at path {a} ".format(a= path+'\\'+output_file))
    except Exception as e:
        log.error("Error saving CSV file: %s", str(e))


# def schema_check(df1, df2):
#     if len(df1.columns) != len(df2.columns):
#         log.info("Number of Columns are different")
#         log.info("Impala Columns are {a}".format(a=len(df1.columns)) )
#         log.info("Presto Columns are {a}".format(a=len(df2.columns)))
#         # log.info("Impala Columns are {a}".format(a=df1.columns) )
#         # log.info("Presto Columns are {a}".format(a=df2.columns))


#         return False

#     dtypes1 = df1.dtypes
#     dtypes2 = df2.dtypes

#     if not dtypes1.equals(dtypes2):
#         log.info("Data types | Names of columns are different.")
#         log.info("Impala Columns are {a}".format(a=df1.columns) )
#         log.info("Presto Columns are {a}".format(a=df2.columns))
#         return False

#     return True

def schema_check(df1, df2):
    if len(df1.columns) != len(df2.columns):
        log.info("Number of Columns are different")
        log.info("Impala Columns are {a}".format(a=len(df1.columns)) )
        log.info("Presto Columns are {a}".format(a=len(df2.columns)))
        # log.info("Impala Columns are {a}".format(a=df1.columns) )
        # log.info("Presto Columns are {a}".format(a=df2.columns))


        return False
    for col1, col2 in zip(df1.columns, df2.columns):
        if df1[col1].dtype != df2[col2].dtype:
            print(f"Data type mismatch for columns {col1} and {col2}.")
            log.info("Data types | Names of columns are different.")
            log.info("Impala Columns are {a}".format(a=df1.columns) )
            log.info("Presto Columns are {a}".format(a=df2.columns))
            return False

    return True

def analyze_date_format(df):
    date_col = None

    # Check if df has a date column
    if any(df.dtypes == 'date'):
        date_col = df.select_dtypes(include='date').columns[0]

    # Analyze date format
    if date_col is not None:
        date_format = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d').iloc[0]
        return date_format

    return None

def validate_date_columns(df1, df2):
    date_format1 = analyze_date_format(df1)
    date_format2 = analyze_date_format(df2)

    # Check if both dataframes have a date column
    if date_format1 is None or date_format2 is None:
        log.info("Both dataframes must have a date column.")

        return '',

    # Check if the date formats are the same
    if date_format1 != date_format2:
        log.info("The date formats are different.")

        return False 

    return True

def getimpaladata(conn, query):
        
    starttime = datetime.now()
    conn.execute(query)
    columns = conn.description
    result1 = [{columns[index][0]: column for index, column in enumerate(value)} for value in conn.fetchall()]
    # result1 = impala_cursor.fetchall()
    conn.close()
    endtime = datetime.now()
    statistics['executiontime_impala'] = endtime - starttime

    df = pd.DataFrame(result1)
    return df

    
def getprestodata(conn, query):
    
    starttime = datetime.now()
    conn.execute(query)
    presto_columns = conn.description
    cols=[[column[0] for column in presto_columns]]
    result2 = conn.fetchall()
    conn.close()
    endtime = datetime.now()
    statistics['executiontime_presto'] = endtime - starttime
    df = pd.DataFrame(result2,columns=cols)
    
    return df

def validate_string_length_df1(df1,df2 , column_list :list):
    len_stats= {}
    valid = True
    for onecolumn in column_list:
         
        lengths1 = df1[onecolumn].apply(lambda x: len(str(x))).sum().sum()
        lengths2 = df2[onecolumn].apply(lambda x: len(str(x))).sum().sum()
        if lengths1 == lengths2:
            len_stats[onecolumn] = True 
        else:
            len_stats[onecolumn] = False 
            valid = False

            
    return len_stats, valid


def validate_data_frames(df1, df2, executionId, filename,exec_time_impala,exec_time_presto):
    print('inside validate_data_frames')
    statistics['executionid'] = executionId
    statistics['filename'] = filename

    statistics['validaterows'] = len(df1) == len(df2)
    statistics['rowcount_impala'] = len(df1)
    statistics['rowcount_presto'] = len(df2)

    schema_check_result = schema_check(df1, df2)
    statistics['schema_check'] = schema_check_result
   
    try:
        if schema_check_result:
            string_columns_df = df1.select_dtypes(include=[object]).columns.tolist()
            print(string_columns_df)
            string_Stats , valid =  validate_string_length_df1(df1,df2 ,string_columns_df )
            # print(string_Stats)
            if valid :
                statistics['String_len_validation'] = True 
            else :
                statistics['String_len_validation'] = False 
    except:
        statistics['String_len_validation'] = False 
  
    numeric_columns_df1 = df1.select_dtypes(include=[float, int]).columns.tolist()
    statistics['numeric_columns_impala'] = ', '.join(numeric_columns_df1)
    statistics['mean_impala'] = df1[numeric_columns_df1].mean().mean()

    numeric_columns_df2 = df2.select_dtypes(include=[float, int]).columns.tolist()
    statistics['numeric_columns_presto'] = ', '.join(numeric_columns_df2)
    statistics['mean_presto'] = df2[numeric_columns_df2].mean().mean()
    
    datevalidation = validate_date_columns(df1,df2)
    statistics['validate_datetime_format'] = datevalidation[0]
    
    # statistics['DateTime'] = datetime.now()

    statistics['sum_impala'] = df1[numeric_columns_df1].sum().sum()
    statistics['min_impala'] = df1[numeric_columns_df1].min().min()
    statistics['max_impala'] = df1[numeric_columns_df1].max().max()
    statistics['min_presto'] = df2[numeric_columns_df2].min().min()
    statistics['max_presto'] = df2[numeric_columns_df2].max().max()
    statistics['sum_presto'] = df2[numeric_columns_df2].sum().sum()
    # statistics['date_time'] = str(time.time())
    
    statistics['variation_time (sec)'] = abs(exec_time_presto - exec_time_impala)

    final_df.append(statistics.copy())
    return final_df


# def validate(df, impalaconnection: dict, prestoconnection: dict):

#     impala_cursor = connectImpala(impalaconnection)
#     if not impala_cursor:
#         return

#     presto_cursor = connectPresto(prestoconnection)
#     if not presto_cursor:
#         return

def validate(df, impalaconnection: dict, prestoconnection : dict):
    # log.info("validation started")

    impala_cursor = connectImpala(impalaconnection)
    presto_cursor = connectPresto(prestoconnection)
    


    for i, onedf in df.iterrows():

        executionId = onedf['executionId']
        print(executionId)
        # print(time.time())
        # log.info("Validation started for executionId ", str(executionId))
        log.info("Validation started for executionId {a} :".format(a=executionId))

        filename = onedf['file']
        print(filename)

        try:
            # impala_data = pd.read_excel(r'C:\Users\salonydubey\OneDrive - KPMG\translator\ImpalaResult.xlsx')
            # presto_data = pd.read_excel(r'C:\Users\salonydubey\OneDrive - KPMG\translator\prestoResult.xlsx')
            impala_start_date = time.time()
            try:
                impala_cursor.execute(onedf['query'])
                columns = impala_cursor.description
                result1 = [{columns[index][0]: column for index, column in enumerate(value)} for value in impala_cursor.fetchall()]
                impala_end_date = time.time()
                # result1 = impala_cursor.fetchall()
                exec_time_impala = impala_end_date - impala_start_date
                exec_time_impala = round(exec_time_impala,3)
                statistics['execution_time_impala (sec)'] = str(exec_time_impala)
                df1 = pd.DataFrame(result1)
                print(df1)
            except Exception as e:
                print(e)
    
            presto_start_date = time.time()
            presto_cursor.execute(onedf['translated_query'])
            presto_columns = presto_cursor.description
            # result2 = [{presto_columns[index][0]: column for index, column in enumerate(value)} for value in presto_cursor.fetchall()]
            cols=[]
            for index, column in enumerate(presto_columns): 
                cols.append(column[0])
            result2 = presto_cursor.fetchall()
            presto_end_date = time.time()
            exec_time_presto = presto_end_date - presto_start_date
            exec_time_presto = round(exec_time_presto,3)
            statistics['execution_time_presto (sec)'] = str(exec_time_presto)
            df2 = pd.DataFrame(result2,columns=cols)
            print(df2)

            # impala_data = getimpaladata(impala_cursor,onedf['query'] )
            # presto_data = getprestodata(presto_cursor,onedf['translated_query'] )

                     
            final_df = validate_data_frames(df1, df2, executionId, filename,exec_time_impala,exec_time_presto)
        except Exception as e:
            log.error("Error occurred during validation: %s", str(e))
            return

    save_csv(r'C:\Impress\sql_translate','validator_stats.csv',final_df)


