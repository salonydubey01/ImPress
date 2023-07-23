#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 13:51:56 2023

@author: salonydubey
"""
from Validator_spark import validate
from pyspark.sql import SparkSession
import pandas as pd
import os
import logging

from presto import *
from pyhive import presto 
from impala.dbapi import connect as impala_connect

# # Create a SparkSession
spark = SparkSession.builder.getOrCreate()


#provide the path where all the files as text is available
impalafilepath=''


log = logging.getLogger(__name__)

impalaconnection = {"host":"65.0.120.79",
                      "port":"21050"}

prestoconnection = {"host":"",
                      "port":"8889",
                      "catalog":"hive",
                      "schema":"presto_db"}



def read_from_folder(filename, directory):
    if not filename.endswith(".txt"):
        raise ValueError("Please provide a TEXT file")

    filepath = os.path.join(str(directory), filename)
   # ! logging.info("filepath: %s", filepath)

    with open(filepath, "r") as f:
        query = f.read()

    return query, filename
# data = [('SELECT employee_id, first_name, last_name from employee1 order by first_name','SELECT employee_id, first_name, last_name from employee1 order by first_name'),('select birthdate from employee3','select birthdate as birthday from employee3_new')]
# columns  = ['query','translated_query']
# df = spark.createDataFrame(data,columns)

# validate(df, spark, impalaconnection, prestoconnection)


# """
# python KPMGTranslator.py -v "Yes" -i "/Users/salonydubey/Downloads/impress_new/impala_queries_validator" -p "/Users/salonydubey/Downloads/impress_new/impala_queries_validator_output" -ih "15.206.207.35" -ip "21050" -ph "3.110.105.136" -pp "8889" -pc "hive" -ps "presto_db"

# """

# "-v" "Yes" "-i" "/Users/salonydubey/Downloads/impress_new/impala_queries_validator" "-p" "/Users/salonydubey/Downloads/impress_new/impala_queries_validator_output" "-ih" "15.206.207.35" "-ip" "21050" "-ph" "3.110.105.136" "-pp" "8889" "-pc" "hive" "-ps" "presto_db"
# """

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

data ={     'file':[],
             'query' :[],
             'translated_query':[],
             'executionId' :[]}


schema = StructType([
    StructField("file", StringType(), True),
    StructField("query", StringType(), True),
    StructField("translated_query", StringType(), True),
    StructField("executionId", StringType(), True)
])

#prestofilepath ='/Users/salonydubey/Downloads/impress_new/impala_queries_validator_output'

def getdataframe(impalafilepath):
   
   
    data ={     'file':[],
                 'query' :[],
                 'translated_query':[],
                 'executionId' :[]}
    
    
    schema = StructType([
        StructField("file", StringType(), True),
        StructField("query", StringType(), True),
        StructField("translated_query", StringType(), True),
        StructField("executionId", StringType(), True)
    ])   
   
    for file in os.listdir(impalafilepath):
    
        try :
            impala_query, filename = read_from_folder(file, impalafilepath)
            presto_query, filename = read_from_folder(file, prestofilepath)
            executionId = ''
            data['query'].append(impala_query)
            data['executionId'].append(executionId)
            data['file'].append(filename)
            data['translated_query'].append(presto_query)
            
            
        except Exception as e :
            print()
            continue
        
    
    data_list = list(zip(data['file'], data['query'], data['translated_query'], data['executionId']))
    
    df = spark.createDataFrame(data_list, schema)

    return df
#df = pd.DataFrame.from_dict(data)

def runvalidator(impalaconnection : dict ,prestoconnection : dict, impalafilepath  ):

    try:
        impala_cursor = connectImpala(impalaconnection)
        presto_cursor = connectPresto(prestoconnection)
        
        df = getdataframe(impalafilepath)
    
        validate(df, spark, impala_cursor, presto_cursor)
    except Exception as e:
               log.error("Error occurred during validation : %s", str(e))
               return




